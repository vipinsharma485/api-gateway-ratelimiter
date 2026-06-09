# ADR-0001: Distributed rate limiting via Redis sliding window

**Status:** Accepted
**Date:** 2026-06-09
**Decision driver:** Vipin Sharma

## Context

The gateway needs to enforce request-rate limits per user/API key/IP, both to protect downstream services from accidental misuse and to provide a contractual tier guarantee to API consumers.

The current implementation (Phase 1 → 2) uses an **in-memory token bucket** held within each gateway pod. This works correctly when a single gateway instance handles all traffic, which is true in local development with `docker compose`.

In production the gateway runs **multiple replicas behind a Kubernetes Service**. With per-pod in-memory state:

- Each pod independently maintains its own counters
- A user limited to 100 req/min could send 100 req/min *to each pod*
- With N pods, the effective rate is N × the intended limit
- Rolling deployments reset counters mid-flight, briefly allowing burst over-the-limit traffic
- Sticky sessions partially mitigate this but break under pod failures and complicate load balancing

This is a correctness bug under horizontal scaling, not a performance issue. It must be resolved before the system can be deployed beyond a single instance.

## Decision

Move rate-limit state to **Redis**, accessed by all gateway pods. Use a **sliding-window log algorithm** implemented as a **single atomic Lua script** server-side.

The algorithm uses a Redis sorted set per rate-limit key:

1. Score = request timestamp (millis); Member = unique request ID
2. On each incoming request, the Lua script:
   - Removes entries older than `(now - window)` via `ZREMRANGEBYSCORE`
   - Counts current entries via `ZCARD`
   - If under limit: adds new entry via `ZADD`, returns ALLOW
   - If over limit: returns DENY
3. `EXPIRE` is set on the key to bound memory growth

Why Lua: each request must be a single atomic check-and-add. Doing this with separate Redis commands (even within MULTI/EXEC) creates a check-then-act race condition between concurrent requests on the same key. Lua scripts execute atomically in Redis, preventing this.

## Considered alternatives

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Sticky sessions** | No infra change | Breaks under pod failure; uneven load distribution; doesn't solve rolling deploys | Rejected — incomplete |
| **Fixed-window counter in Redis (INCR + EXPIRE)** | Simple; ~2 Redis calls | Boundary spike: 100 reqs at second 59 + 100 at second 0 = 200/min despite "100/min" limit | Rejected — incorrect under burst patterns |
| **Sliding window log with Lua (chosen)** | Correct under bursts; atomic; ~1 round-trip | Slightly higher memory than counter (one ZSET entry per request in window) | **Accepted** |
| **Sliding window counter (Redis Bloom)** | Probabilistic, low memory | False positives at scale; requires RedisBloom module | Rejected — adds dep, harder to reason about |
| **Distributed token bucket (e.g., Bucket4j+Hazelcast)** | Token bucket semantics natively | Adds Hazelcast as new infra dependency; team unfamiliar | Rejected — complexity not justified |

## Consequences

### Positive
- Correct rate limiting across N gateway pods regardless of which pod serves a given request
- Counters survive rolling deploys and pod restarts
- Single source of truth for rate-limit state — debuggable via `redis-cli`
- Scales horizontally: Redis Cluster supports key-based sharding when single-instance Redis becomes a bottleneck

### Negative
- Adds Redis as a hard dependency. Outage of Redis affects gateway availability — mitigated by fail-open policy (see ADR-0002, *planned*)
- Each request now has a ~1 ms Redis round-trip overhead. Acceptable trade-off; measured in Phase 4 results
- Memory per rate-limit key scales with window size × peak request rate. Need to monitor; consider truncation strategies if it grows unbounded

### Open questions tracked for future ADRs
- **ADR-0002 (planned):** Fail-open vs fail-closed when Redis is unreachable
- **ADR-0003 (planned):** Rate-limit key strategy (user ID > API key > IP fallback)
- **ADR-0004 (planned):** When to shard Redis: keys vs cluster mode

## References

- *Designing Data-Intensive Applications*, Kleppmann — Chapter 5 (Replication) and 9 (Consistency)
- Redis docs: [EVAL and Lua scripting](https://redis.io/docs/manual/programmability/eval-intro/)
- Stripe Engineering: [Scaling your API with rate limiters](https://stripe.com/blog/rate-limiters)
- Cloudflare: [How we built rate limiting capable of scaling to millions of domains](https://blog.cloudflare.com/counting-things-a-lot-of-different-things/)

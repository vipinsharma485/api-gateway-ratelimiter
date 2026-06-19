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
- **ADR-0003 (planned):** Fail-open vs fail-closed when Redis is unreachable
- **ADR-0004 (planned):** Rate-limit key strategy (user ID > API key > IP fallback)
- **ADR-0005 (planned):** When to shard Redis: keys vs cluster mode

(ADR-0002 is the circuit-breaker decision — a separate resilience concern.)

## Phase 4 Update — Sliding Window via Lua Script

Phase 3 shipped a Redis fixed-window counter; Phase 4 replaces it with the sliding-window log this ADR originally specified and makes that the production limiter. The journey fixes two distinct problems in order: first atomicity (the token-bucket counters before it could race), then fairness (a fixed window bursts at its boundaries). This section records both.

### The atomicity problem: how a naive token bucket races

The token bucket read and wrote Redis in separate steps: roughly a `GET` of the remaining tokens, a decision in the gateway JVM, then a `DECREMENT`. Those steps are not a single atomic operation. Two requests for the same key can both `GET` the same remaining count (say `1`), both decide they are under the limit, and both `DECREMENT` — letting two requests through a bucket that only had room for one. This is a classic check-then-act race, and it gets worse, not better, as traffic and replica count grow. Wrapping the commands in `MULTI`/`EXEC` does not fix it: a transaction batches commands but the *decision* still depends on a value read before the batch, so the read-decide-write window is still open to interleaving.

### How the Lua script eliminates it

Redis executes a Lua script as a single atomic unit — no other command from any client can interleave while the script runs. The `sliding_window.lua` script does the eviction (`ZREMRANGEBYSCORE`), the count (`ZCARD`), the decision, and the write (`ZADD` + `PEXPIRE`) all inside that atomic execution. There is no read-decide-write gap for a concurrent request to exploit, so no `MULTI`/`EXEC` and no optimistic `WATCH` retry loop is needed. Atomicity is the whole point of moving the logic server-side.

### Why sliding window is fairer than fixed window

A fixed-window counter resets the count at fixed boundaries (e.g. on the minute). A client can send the full quota in the last moment of one window and the full quota again in the first moment of the next — up to 2× the intended rate across that boundary. The sliding-window log scores every request by its own timestamp and, on each request, only counts entries within `[now − window, now]`. The window moves continuously with the clock, so there is no boundary to burst across; the limit holds over every window position, not just the aligned ones.

This fixed-window counter is implemented as `RedisFixedWindowRateLimiter` (Phase 3) — an atomic `INCR` + `PEXPIRE` Lua script over a per-bucket key — and kept **disabled by default**. Keeping it in the codebase lets the boundary burst be reproduced directly and contrasted with the sliding window; note that fixed window is atomic too, so this isolates the *fairness* problem from the *atomicity* problem the token bucket had.

### Trade-off: memory

The token bucket stores `O(1)` per key (a couple of counters). The sliding-window log stores one sorted-set entry per request currently inside the window, i.e. `O(requests-in-window)` per key. With a 1-minute window and a 10-req limit that is at most ~10 small entries per client, bounded by the per-key `PEXPIRE`. This is a deliberate, acceptable cost for exact, burst-correct limiting; if a key's window ever needs to hold a very large number of requests, the approximate sliding-window-counter variant becomes the better memory trade-off (noted in the alternatives table).

### Decision

The **sliding-window Lua script is the production rate limiter** (`RedisSlidingWindowRateLimiter` + `SlidingWindowRateLimiterFilter`). The **Phase 3 fixed-window** limiter (`RedisFixedWindowRateLimiter`) is **kept in the codebase, disabled by default, as a documented comparison** — it is atomic like the sliding window, so it isolates the *fairness* problem (the boundary burst) from the *atomicity* problem the token bucket had.

Both custom limiters are toggleable by configuration: `rate-limiter.fixed-window.enabled` (default `false`) and `rate-limiter.sliding-window.enabled` (default `true`), each a `@ConditionalOnProperty` flag that adds or removes both the limiter bean and its global filter. The built-in Spring Cloud Gateway `RequestRateLimiter` (a Redis token bucket explored earlier) stays commented on `product-route` as a third reference; re-enabling it means uncommenting that block. Shipped defaults keep exactly one limiter active (the sliding window), so there is no double-limiting.

## References

- *Designing Data-Intensive Applications*, Kleppmann — Chapter 5 (Replication) and 9 (Consistency)
- Redis docs: [EVAL and Lua scripting](https://redis.io/docs/manual/programmability/eval-intro/)
- Stripe Engineering: [Scaling your API with rate limiters](https://stripe.com/blog/rate-limiters)
- Cloudflare: [How we built rate limiting capable of scaling to millions of domains](https://blog.cloudflare.com/counting-things-a-lot-of-different-things/)

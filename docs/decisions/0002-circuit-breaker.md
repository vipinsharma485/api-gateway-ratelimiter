# ADR-0002: Circuit breaker on downstream calls (Resilience4j)

**Status:** Accepted
**Date:** 2026-06-18
**Decision driver:** Vipin Sharma

## Context

The gateway proxies requests to downstream services (currently `product-service`). When a downstream is slow or failing, naive proxying makes the problem worse:

- Each inbound request holds a connection/thread waiting on the struggling downstream, so latency and resource use climb on the gateway itself
- A downstream that is timing out turns into gateway-wide latency, which can cascade to *every* route and caller
- Callers receive raw connection errors or hung requests instead of a fast, predictable response

This is the classic case for a **circuit breaker**: detect that a dependency is unhealthy, stop calling it for a cooldown period, and fail fast with a controlled response — then probe for recovery before fully closing again.

## Decision

Wrap downstream routes with a **Resilience4j circuit breaker**, applied through Spring Cloud Gateway's built-in `CircuitBreaker` route filter, with a `fallbackUri` to a local fallback endpoint.

- Dependency: `spring-cloud-starter-circuitbreaker-reactor-resilience4j` (reactive, matching the WebFlux gateway)
- The `product-route` gains a `CircuitBreaker` filter named `productCircuitBreaker` with `fallbackUri: forward:/fallback/products`
- `FallbackController` serves `/fallback/products`, returning **`503 Service Unavailable`** with a small JSON body (`{"error": ..., "retryable": true}`) — fast and explicit rather than a hung request or a leaked error

### Configuration and why

| Setting | Value | Reason |
|---------|-------|--------|
| `sliding-window-type` | `COUNT_BASED` | Judge health over the last N calls, simple and predictable |
| `sliding-window-size` | `10` | Recent-history window the failure rate is computed over |
| `minimum-number-of-calls` | `5` | Don't trip on a tiny sample (1 failed call out of 1) |
| `failure-rate-threshold` | `50%` | Open when half of recent calls fail — a clear sign of trouble |
| `wait-duration-in-open-state` | `10s` | Cooldown before probing, gives the downstream room to recover |
| `permitted-number-of-calls-in-half-open-state` | `3` | Small probe burst to confirm recovery before closing |
| `timelimiter.timeout-duration` | `3s` | A slow call counts as a failure, so latency (not just errors) trips the breaker |

## Considered alternatives

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **No circuit breaker** | Simplest | Cascading latency/failure; poor caller experience | Rejected |
| **Client-side timeouts only** | Easy | Bounds a single call but doesn't stop hammering a dead dependency | Rejected — necessary but not sufficient |
| **Resilience4j via SCG `CircuitBreaker` filter (chosen)** | Idiomatic for SCG; declarative per route; reactive; fallback routing built in | One more dependency and tuning surface | **Accepted** |
| **Retries instead of breaking** | Hides transient blips | Amplifies load on an already-struggling downstream | Rejected as the primary mechanism (retry can be added later, bounded) |

## Consequences

### Positive
- A failing/slow downstream fails fast with a clean `503` instead of cascading latency across the gateway
- The breaker auto-recovers (open → half-open → closed) without manual intervention
- Fallback behaviour is explicit and testable, and gives a clear place to add richer degraded responses later (e.g. cached data)

### Negative
- Adds a dependency and a tuning surface (thresholds/timeouts) that must be set per downstream and revisited under real traffic
- A mis-tuned breaker can open too eagerly (false trips) or too late; the values above are starting points to validate with load tests (Phase 15)

### Open questions tracked for future ADRs
- Per-route vs shared circuit-breaker instances as more downstreams are added
- Whether to layer a bounded retry (with backoff) ahead of the breaker for transient errors
- Surfacing breaker state transitions as metrics/events (ties into the Prometheus and Kafka phases)

## References

- Resilience4j docs: [CircuitBreaker](https://resilience4j.readme.io/docs/circuitbreaker)
- Spring Cloud Gateway: [Circuit Breaker filter](https://docs.spring.io/spring-cloud-gateway/reference/spring-cloud-gateway/gatewayfilter-factories/circuitbreaker-filter-factory.html)
- Nygard, *Release It!* — Circuit Breaker and Bulkhead stability patterns

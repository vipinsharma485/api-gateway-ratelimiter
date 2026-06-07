package com.example.api_gateway;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A GlobalFilter runs on EVERY request passing through the gateway.
 * This one counts requests per user and rejects them past a limit.
 */
@Component
public class InMemoryRateLimitFilter implements GlobalFilter, Ordered {

    private static final int    LIMIT     = 5;        // max 5 requests...
    private static final long   WINDOW_MS = 60_000;   // ...per 60 seconds

    // A thread-safe map: user -> their counter. Lives in memory only.
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        // Identify the caller. We use a header "X-User-Id". No header = "anonymous".
        String user = exchange.getRequest().getHeaders().getFirst("X-User-Id");
        if (user == null) user = "anonymous";

        Counter counter = counters.computeIfAbsent(user, k -> new Counter());

        boolean allowed;
        synchronized (counter) {             // one user's counter updated safely
            long now = System.currentTimeMillis();
            if (now - counter.windowStart > WINDOW_MS) {
                counter.windowStart = now;   // start a fresh 60s window
                counter.count = 0;
            }
            counter.count++;
            allowed = counter.count <= LIMIT;
        }

        if (!allowed) {
            // Reject: set status 429 and stop here (don't forward to backend).
            exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
            return exchange.getResponse().setComplete();
        }
        return chain.filter(exchange);       // allowed → continue to backend
    }

    @Override
    public int getOrder() {
        return -1;   // run early, before routing to the backend
    }

    /** Simple holder for one user's count + when their window started. */
    static class Counter {
        long windowStart = System.currentTimeMillis();
        int  count = 0;
    }
}
package com.example.api_gateway.filter;

import com.example.api_gateway.ratelimit.RedisSlidingWindowRateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Global gateway filter that enforces the distributed sliding-window rate limit
 * on every request before it is routed to a backend service.
 */
@Component
@ConditionalOnProperty(name = "rate-limiter.sliding-window.enabled", havingValue = "true", matchIfMissing = true)
public class SlidingWindowRateLimiterFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(SlidingWindowRateLimiterFilter.class);
    private static final String REJECTION_BODY = "Rate limit exceeded. Try again later.";

    private final RedisSlidingWindowRateLimiter rateLimiter;

    public SlidingWindowRateLimiterFilter(RedisSlidingWindowRateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String clientIp = resolveClientIp(exchange);
        return rateLimiter.isAllowed(clientIp).flatMap(allowed -> {
            if (allowed) {
                // Advertise the limit on the way through (best effort).
                exchange.getResponse().getHeaders()
                        .set("X-RateLimit-Limit", String.valueOf(rateLimiter.getMaxRequests()));
                return chain.filter(exchange);
            }
            return rejectWithTooManyRequests(exchange, clientIp);
        });
    }

    private Mono<Void> rejectWithTooManyRequests(ServerWebExchange exchange, String clientIp) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.TOO_MANY_REQUESTS);

        HttpHeaders headers = response.getHeaders();
        headers.set("X-RateLimit-Limit", String.valueOf(rateLimiter.getMaxRequests()));
        headers.set("X-RateLimit-Remaining", "0");          // best effort: we are at the limit
        headers.set(HttpHeaders.RETRY_AFTER, "60");
        headers.setContentType(MediaType.TEXT_PLAIN);

        log.debug("Rejected request from {} with HTTP 429", clientIp);

        byte[] bytes = REJECTION_BODY.getBytes(StandardCharsets.UTF_8);
        DataBuffer buffer = response.bufferFactory().wrap(bytes);
        return response.writeWith(Mono.just(buffer));
    }

    private String resolveClientIp(ServerWebExchange exchange) {
        InetSocketAddress remoteAddress = exchange.getRequest().getRemoteAddress();
        if (remoteAddress != null && remoteAddress.getAddress() != null) {
            return remoteAddress.getAddress().getHostAddress();
        }
        return "unknown";
    }

    @Override
    public int getOrder() {
        return 1;   // run early, before routing forwards the request to a backend
    }
}

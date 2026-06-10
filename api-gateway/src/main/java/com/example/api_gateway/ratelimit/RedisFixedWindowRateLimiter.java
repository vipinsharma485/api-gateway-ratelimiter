package com.example.api_gateway.ratelimit;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Distributed fixed-window-counter rate limiter (Phase 3).
 *
 * <p>This is the originally planned comparison to the Phase 4 sliding window.
 * Each client gets one Redis counter per fixed time bucket
 * ({@code floor(now / window)}); the counter is incremented atomically with
 * {@code INCR} and expires after one window. It is atomic and cheap
 * ({@code O(1)} per key) but allows a boundary burst: a caller can use a full
 * quota at the end of one bucket and another full quota at the start of the
 * next. That fairness gap is exactly what the sliding-window log fixes, so this
 * limiter ships <strong>disabled by default</strong> and is kept for comparison.
 */
@Component
@ConditionalOnProperty(name = "rate-limiter.fixed-window.enabled", havingValue = "true")
public class RedisFixedWindowRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RedisFixedWindowRateLimiter.class);

    /** Every fixed-window key is namespaced under this prefix. */
    private static final String KEY_PREFIX = "rate_limit:fixed:";

    private final ReactiveRedisTemplate<String, String> redisTemplate;

    @Value("${rate-limiter.fixed-window.window-size-ms:60000}")
    private long windowSizeMs;

    @Value("${rate-limiter.fixed-window.max-requests:10}")
    private int maxRequests;

    /** Loaded once from the classpath; executed atomically inside Redis. */
    private RedisScript<Long> fixedWindowScript;

    public RedisFixedWindowRateLimiter(ReactiveRedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    void loadScript() {
        this.fixedWindowScript =
                RedisScript.of(new ClassPathResource("scripts/fixed_window.lua"), Long.class);
        log.info("Fixed-window rate limiter ready (windowSizeMs={}, maxRequests={})",
                windowSizeMs, maxRequests);
    }

    /**
     * @param clientId logical caller identity (here, the client IP)
     * @return {@code true} while the request count within the current fixed
     *         window is at or below the limit, {@code false} once it exceeds it.
     */
    public Mono<Boolean> isAllowed(String clientId) {
        long now = System.currentTimeMillis();
        long bucket = now / windowSizeMs;                       // the fixed window this request lands in
        String key = KEY_PREFIX + clientId + ":" + bucket;

        List<String> keys = List.of(key);
        List<String> args = List.of(Long.toString(windowSizeMs));

        return redisTemplate.execute(fixedWindowScript, keys, args)
                .next()
                .defaultIfEmpty(0L)
                .map(count -> {
                    boolean allowed = count <= maxRequests;
                    if (!allowed) {
                        log.warn("Rate limit exceeded for clientId={} (current count={}, limit={})",
                                clientId, count, maxRequests);
                    }
                    return allowed;
                });
    }

    public int getMaxRequests() {
        return maxRequests;
    }

    public long getWindowSizeMs() {
        return windowSizeMs;
    }
}

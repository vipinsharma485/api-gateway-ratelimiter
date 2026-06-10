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
import java.util.UUID;

/**
 * Distributed, sliding-window rate limiter backed by a Redis sorted set.
 *
 * <p>The whole check-and-add is delegated to a Lua script
 * ({@code scripts/sliding_window.lua}) that Redis executes atomically, which is
 * the key correctness property the earlier token-bucket approach lacked: a
 * read-then-write across multiple round trips can race between concurrent
 * requests on the same key, but a Lua script cannot interleave with anything.
 */
@Component
@ConditionalOnProperty(name = "rate-limiter.sliding-window.enabled", havingValue = "true", matchIfMissing = true)
public class RedisSlidingWindowRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RedisSlidingWindowRateLimiter.class);

    /** Every rate-limit key is namespaced under this prefix. */
    private static final String KEY_PREFIX = "rate_limit:";

    private final ReactiveRedisTemplate<String, String> redisTemplate;

    @Value("${rate-limiter.sliding-window.window-size-ms:60000}")
    private long windowSizeMs;

    @Value("${rate-limiter.sliding-window.max-requests:10}")
    private int maxRequests;

    /** Loaded once from the classpath; executed atomically inside Redis. */
    private RedisScript<Long> slidingWindowScript;

    public RedisSlidingWindowRateLimiter(ReactiveRedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    void loadScript() {
        this.slidingWindowScript =
                RedisScript.of(new ClassPathResource("scripts/sliding_window.lua"), Long.class);
        log.info("Sliding-window rate limiter ready (windowSizeMs={}, maxRequests={})",
                windowSizeMs, maxRequests);
    }

    /**
     * Decides whether the request for {@code clientId} is within the configured
     * limit.
     *
     * @param clientId logical caller identity (here, the client IP)
     * @return {@code true} if the request is allowed, {@code false} if it must be
     *         rejected. The decision and the bookkeeping happen as one atomic
     *         operation in Redis.
     */
    public Mono<Boolean> isAllowed(String clientId) {
        String key = KEY_PREFIX + clientId;
        long now = System.currentTimeMillis();

        // The score is the timestamp (window math); the member must be unique
        // because several requests can land within the same millisecond.
        String member = now + "-" + UUID.randomUUID();

        List<String> keys = List.of(key);
        List<String> args = List.of(
                Long.toString(now),
                Long.toString(windowSizeMs),
                Integer.toString(maxRequests),
                member);

        return redisTemplate.execute(slidingWindowScript, keys, args)
                .next()
                .defaultIfEmpty(0L)
                .flatMap(result -> {
                    boolean allowed = result == 1L;
                    if (allowed) {
                        return Mono.just(Boolean.TRUE);
                    }
                    // Rejected path: read the current count only to enrich the log.
                    return redisTemplate.opsForZSet().size(key)
                            .defaultIfEmpty(0L)
                            .doOnNext(currentCount -> log.warn(
                                    "Rate limit exceeded for clientId={} (current count={}, limit={})",
                                    clientId, currentCount, maxRequests))
                            .thenReturn(Boolean.FALSE);
                });
    }

    public int getMaxRequests() {
        return maxRequests;
    }

    public long getWindowSizeMs() {
        return windowSizeMs;
    }
}

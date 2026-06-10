package com.example.api_gateway.ratelimit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.test.StepVerifier;

import java.util.UUID;

/**
 * Integration test for {@link RedisFixedWindowRateLimiter} against a real Redis
 * started by Testcontainers. The fixed-window limiter is disabled by default,
 * so the test enables it via {@code rate-limiter.fixed-window.enabled=true}.
 *
 * <p>Each test aligns to the start of a fresh window before firing a burst so
 * the burst cannot accidentally straddle a bucket boundary.
 */
@SpringBootTest
@Testcontainers
class RedisFixedWindowRateLimiterTest {

    private static final long WINDOW_MS = 1000L;
    private static final int MAX_REQUESTS = 5;

    @Container
    static final GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                    .withExposedPorts(6379);

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("rate-limiter.fixed-window.enabled", () -> "true");
        registry.add("rate-limiter.fixed-window.window-size-ms", () -> WINDOW_MS);
        registry.add("rate-limiter.fixed-window.max-requests", () -> MAX_REQUESTS);
    }

    @Autowired
    private RedisFixedWindowRateLimiter rateLimiter;

    @Test
    void allowsRequestsWithinTheLimit() throws InterruptedException {
        alignToWindowStart();
        String clientId = "within-" + UUID.randomUUID();
        for (int i = 0; i < MAX_REQUESTS; i++) {
            StepVerifier.create(rateLimiter.isAllowed(clientId))
                    .expectNext(true)
                    .verifyComplete();
        }
    }

    @Test
    void rejectsRequestsThatExceedTheLimit() throws InterruptedException {
        alignToWindowStart();
        String clientId = "exceed-" + UUID.randomUUID();
        for (int i = 0; i < MAX_REQUESTS; i++) {
            StepVerifier.create(rateLimiter.isAllowed(clientId))
                    .expectNext(true)
                    .verifyComplete();
        }
        StepVerifier.create(rateLimiter.isAllowed(clientId))
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void resetsTheCountAtTheNextWindow() throws InterruptedException {
        alignToWindowStart();
        String clientId = "reset-" + UUID.randomUUID();
        for (int i = 0; i < MAX_REQUESTS; i++) {
            StepVerifier.create(rateLimiter.isAllowed(clientId))
                    .expectNext(true)
                    .verifyComplete();
        }
        StepVerifier.create(rateLimiter.isAllowed(clientId))
                .expectNext(false)
                .verifyComplete();

        // Crossing into the next fixed window resets the counter: a full fresh
        // quota becomes available immediately. This is the fixed-window boundary
        // burst that the Phase 4 sliding window eliminates.
        Thread.sleep(WINDOW_MS + 100L);
        for (int i = 0; i < MAX_REQUESTS; i++) {
            StepVerifier.create(rateLimiter.isAllowed(clientId))
                    .expectNext(true)
                    .verifyComplete();
        }
    }

    /** Sleep until just after the start of a fresh fixed window. */
    private static void alignToWindowStart() throws InterruptedException {
        long offset = System.currentTimeMillis() % WINDOW_MS;
        Thread.sleep(WINDOW_MS - offset + 20L);
    }
}

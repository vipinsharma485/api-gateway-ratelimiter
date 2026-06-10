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
 * Integration test for {@link RedisSlidingWindowRateLimiter} against a real
 * Redis instance started by Testcontainers.
 *
 * <p>A short one-second window is configured so the "window resets" test stays
 * fast. Each test uses a fresh client id so they do not share rate-limit state.
 */
@SpringBootTest
@Testcontainers
class RedisSlidingWindowRateLimiterTest {

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
        registry.add("rate-limiter.sliding-window.window-size-ms", () -> WINDOW_MS);
        registry.add("rate-limiter.sliding-window.max-requests", () -> MAX_REQUESTS);
    }

    @Autowired
    private RedisSlidingWindowRateLimiter rateLimiter;

    @Test
    void allowsRequestsWithinTheLimit() {
        String clientId = "within-" + UUID.randomUUID();
        for (int i = 0; i < MAX_REQUESTS; i++) {
            StepVerifier.create(rateLimiter.isAllowed(clientId))
                    .expectNext(true)
                    .verifyComplete();
        }
    }

    @Test
    void rejectsRequestsThatExceedTheLimit() {
        String clientId = "exceed-" + UUID.randomUUID();
        // The first MAX_REQUESTS requests are allowed...
        for (int i = 0; i < MAX_REQUESTS; i++) {
            StepVerifier.create(rateLimiter.isAllowed(clientId))
                    .expectNext(true)
                    .verifyComplete();
        }
        // ...the (MAX_REQUESTS + 1)th request is rejected.
        StepVerifier.create(rateLimiter.isAllowed(clientId))
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void allowsAgainAfterWindowExpires() throws InterruptedException {
        String clientId = "reset-" + UUID.randomUUID();
        for (int i = 0; i < MAX_REQUESTS; i++) {
            StepVerifier.create(rateLimiter.isAllowed(clientId))
                    .expectNext(true)
                    .verifyComplete();
        }
        // Limit reached: the next request is rejected.
        StepVerifier.create(rateLimiter.isAllowed(clientId))
                .expectNext(false)
                .verifyComplete();

        // Wait for the window to slide past every recorded request.
        Thread.sleep(WINDOW_MS + 300L);

        StepVerifier.create(rateLimiter.isAllowed(clientId))
                .expectNext(true)
                .verifyComplete();
    }
}

package com.example.api_gateway;

import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

@Configuration
public class RateLimiterConfig {
    // Tells the limiter "who" each request is, so counts are per-user.
    @Bean
    public KeyResolver userKeyResolver() {
        return exchange -> {
            String user = exchange.getRequest().getHeaders().getFirst("X-User-Id");
            return Mono.just(user != null ? user : "anonymous");
        };
    }
}
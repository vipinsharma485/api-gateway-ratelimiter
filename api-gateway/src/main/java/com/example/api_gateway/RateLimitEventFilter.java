package com.example.api_gateway;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * Watches the final response. If it's a 429, it drops an event onto Kafka.
 * It sits "outside" the rate-limit filter so it can observe the 429 result.
 */
@Component
public class RateLimitEventFilter implements GlobalFilter, Ordered {

    private static final String TOPIC = "rate-limit-events";
    private final KafkaTemplate<String, String> kafka;

    public RateLimitEventFilter(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;    // Spring provides this automatically
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        // Run the rest of the chain first, THEN inspect the result.
        return chain.filter(exchange).then(Mono.fromRunnable(() -> {
            var status = exchange.getResponse().getStatusCode();
            if (status != null && status.value() == 429) {
                String user = exchange.getRequest().getHeaders().getFirst("X-User-Id");
                String path = exchange.getRequest().getPath().toString();
                String event = "{\"user\":\"" + user + "\",\"path\":\"" + path
                             + "\",\"time\":\"" + Instant.now() + "\"}";
                kafka.send(TOPIC, user, event);          // drop note on the belt
                System.out.println("📤 Kafka event: " + event);
            }
        }));
    }

    @Override
    public int getOrder() {
        return -100;   // very outside → its .then() runs LAST and sees the final 429
    }
}
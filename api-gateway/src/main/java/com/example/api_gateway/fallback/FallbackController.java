package com.example.api_gateway.fallback;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Serves fast, graceful fallback responses when a downstream circuit breaker is
 * open or a call fails/times out. The {@code CircuitBreaker} gateway filter
 * forwards here via its {@code fallbackUri} (e.g. {@code forward:/fallback/products}),
 * so the client receives a clean 503 instead of a hung request or a leaked
 * stack trace.
 */
@RestController
public class FallbackController {

    @RequestMapping("/fallback/products")
    public ResponseEntity<Map<String, Object>> products() {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of(
                        "error", "product-service is temporarily unavailable",
                        "retryable", true));
    }
}

package com.example.api_gateway.fallback;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Plain unit test for the circuit-breaker fallback endpoint - no Spring context,
 * Redis, or Docker required. The breaker's open/half-open behaviour is exercised
 * end-to-end via the documented smoke test.
 */
class FallbackControllerTest {

    private final FallbackController controller = new FallbackController();

    @Test
    void productsFallbackReturns503WithDegradedBody() {
        ResponseEntity<Map<String, Object>> response = controller.products();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody()).containsEntry("retryable", true);
        assertThat(response.getBody()).containsKey("error");
    }
}

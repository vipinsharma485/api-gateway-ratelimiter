package com.example.product_service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@RestController                   
public class ProductController {

    @GetMapping("/products")       
    public List<Map<String, Object>> getProducts() {
        return List.of(
            Map.of("id", 1, "name", "Laptop",  "price", 999),
            Map.of("id", 2, "name", "Mouse",   "price", 25),
            Map.of("id", 3, "name", "Monitor", "price", 199)
        );
    }

    @GetMapping("/products/health")
    public Map<String, String> health() {
        return Map.of("status", "product-service is alive");
    }
}
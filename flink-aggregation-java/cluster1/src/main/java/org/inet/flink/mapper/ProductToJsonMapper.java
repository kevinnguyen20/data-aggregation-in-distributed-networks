package org.inet.flink.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.inet.flink.model.Product;

import java.io.IOException;

public class ProductToJsonMapper implements MapFunction<Product, String> {
    private static final long serialVersionUID = 1L;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String map(Product product) {
        try {
            return objectMapper.writeValueAsString(product);
        } catch (IOException e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }
}

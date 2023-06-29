package org.inet.flink.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.inet.flink.model.Product;

import java.io.IOException;

public class JsonToProductMapper implements MapFunction<String, Product> {
    private static final long serialVersionUID = 1L;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Product map(String json) {
        try {
            return objectMapper.readValue(json, Product.class);
        } catch (IOException e) {
            throw new RuntimeException("Deserialization failed", e);
        }
    }
}

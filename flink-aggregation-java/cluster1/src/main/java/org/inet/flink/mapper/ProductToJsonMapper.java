package org.inet.flink.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.inet.flink.model.Product;
import org.inet.flink.model.Delay;

import java.io.IOException;

public class ProductToJsonMapper implements MapFunction<Product, String> {
    private static final long serialVersionUID = 1L;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Delay delay = new Delay();

    @Override
    public String map(Product product) {
        try {
            double delayDuration = delay.calculateDelay();
            long endTime = System.currentTimeMillis() + (long) (delayDuration);

            while (System.currentTimeMillis()<endTime) {
                // Busy waiting for the delay duration
            }

            return objectMapper.writeValueAsString(product);
        } catch (IOException e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }
}

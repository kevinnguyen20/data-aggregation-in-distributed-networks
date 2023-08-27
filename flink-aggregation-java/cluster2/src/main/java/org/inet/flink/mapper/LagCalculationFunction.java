package org.inet.flink.mapper;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.TimerService;

import org.inet.flink.model.Product;

public class LagCalculationFunction extends ProcessFunction<Product, String> {
    @Override
    public void processElement(Product product, Context context, Collector<String> out) throws Exception {
        // Access the watermark
        long currentWatermark = context.timerService().currentWatermark();
        
        // Calculate the event time lag
        long eventTimeLag = System.currentTimeMillis() - currentWatermark;

        out.collect("Product: " + product.getName() + ", Event Time Lag: " + eventTimeLag + " ms");
    }
}

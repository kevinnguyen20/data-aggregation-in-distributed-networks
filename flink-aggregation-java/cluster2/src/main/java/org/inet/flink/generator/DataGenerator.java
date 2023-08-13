package org.inet.flink.generator;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Properties;

public class DataGenerator {
    private final KafkaProducer<String, String> producer;
    private final List<String> productNames = Arrays.asList("Apple", "Banana", "Lemon", "Cherry", "Melon", "Peach", "Grapefruit");

    public DataGenerator(String kafkaBootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);
    }

    public void generateData(String producerTopic) throws InterruptedException {
        long startTime = System.currentTimeMillis();

        // The "batchSize" controls the amount of records sent to the Apache
        // Flink cluster. This approach is bounded.
        int batchSize = 10000000;
        Random random = new Random();

        for (int i=1; i<=batchSize; i++) {
            String recordValue = toJson(i, getRandomProductName(random), assignRandomPrice(random));
            sendMessage(producerTopic, recordValue);
        }

        // This could be used for debugging purposes
        long endTime = System.currentTimeMillis();
		long elapsedTime = endTime - startTime;
		String recordValue = toJson(0, "Elapsed time", elapsedTime);

        sendMessage(producerTopic, recordValue);
		
		producer.flush();
		producer.close();
    }

    private static String toJson(int id, String productName, double price) {
        String formattedPrice = String.format("%.2f", price);
        return String.format("{\"id\": %d, \"name\": \"%s\", \"price\": %s}", id, productName, formattedPrice);
    }

    private void sendMessage(String producerTopic, String recordValue) {
        ProducerRecord<String, String> record = new ProducerRecord<>(producerTopic, recordValue);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e!=null) {
                    System.out.println(e.getMessage());
                }
            }
        });
    }

    private String getRandomProductName(Random random) {
        return productNames.get(random.nextInt(productNames.size()));
    }

    private double assignRandomPrice(Random random) {
        return 0.5 + random.nextDouble() * (1.8 - 0.5);
    }
}

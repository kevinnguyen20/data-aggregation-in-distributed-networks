package org.inet.flink.generator;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Properties;

public class DataGenerator {
    private final KafkaProducer<String, String> producer;
    private final List<String> productNames;

    /**
     * Constructor for the data generator.
     * @param kafkaBootstrapServers a server on which Kafka works
     */
    public DataGenerator(String kafkaBootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);
        productNames = Arrays.asList("Apple", "Banana", "Lemon", "Cherry", "Melon", "Peach", "Grapefruit");
    }

    // TODO Make it an unbounded data generation
    // TODO Think of more random records, maybe?
    public void generateData(String producerTopic) throws InterruptedException {
        long startTime = System.currentTimeMillis();

        int batchSize = 100;
        Random random = new Random();

        for (int i = 1; i <= batchSize; i++) {
            String recordValue = toJson(i, getRandomProductName(random), assignRandomPrice(random));
            sendMessage(producerTopic, recordValue);
        }

        // This could be used for debugging purposes
        long endTime = System.currentTimeMillis();
		long elapsedTime = endTime - startTime;
		String recordValue = toJson(elapsedTime);

        sendMessage(producerTopic, recordValue);
		
		producer.flush();
		producer.close();
    }

    private static String toJson(int id, String productName, double price) {
        String formattedPrice = String.format("%.2f", price);
        return "{\"id\": " + id + ", \"name\": \"" + productName + "\", \"price\": " + formattedPrice + "}";
    }

    private static String toJson(long elapsedTime) {
        return "{\"id\": " + 0 + ", \"name\": \"Elapsed time\", \"price\":" + elapsedTime + "}";
    }

    /**
     * Sends a record (custom value) to the specified topic.
     * @param producerTopic a receiver topic
     * @param recordValue the value that is sent
     */
    private void sendMessage(String producerTopic, String recordValue) {
        ProducerRecord<String, String> record = new ProducerRecord<>(producerTopic, recordValue);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null) {
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

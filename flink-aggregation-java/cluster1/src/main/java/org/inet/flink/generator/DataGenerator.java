package org.inet.flink.generator;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DataGenerator {
    private final KafkaProducer<String, String> producer;

    public DataGenerator(String kafkaBootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);
    }

    public void generateData(String producerTopic) {
        long startTime = System.currentTimeMillis();

        int batchSize = 10000000;
        for (int i = 1; i <= batchSize; i++) {
            String recordValue = toJson(i);
            sendMessage(producerTopic, recordValue);
        }

        long endTime = System.currentTimeMillis();
		long elapsedTime = endTime - startTime;
		String recordValue = toJson(elapsedTime);
        sendMessage(producerTopic, recordValue);
		
		producer.flush();
		producer.close();
    }

    private static String toJson(int id) {
        return "{\"id\": " + id + ", \"name\": \"Apple\", \"price\": 0.85}";
    }

    private static String toJson(long elapsedTime) {
        return "{\"id\": " + 0 + ", \"name\": \"Apple\", \"price\":" + elapsedTime + "}";
    }

    private void sendMessage(String producerTopic, String recordValue) {
        ProducerRecord<String, String> record = new ProducerRecord<>(producerTopic, recordValue);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null) {
                    e.getMessage();
                }
            }
        });
    }
}

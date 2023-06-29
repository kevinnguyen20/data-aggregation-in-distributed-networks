package org.inet.flink.generator;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class DataGenerator {
    private final KafkaProducer<String, String> producer;

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
    }

    // TODO Make it an unbounded data generation
    // TODO Think of more random records, maybe?
    public void generateData(String producerTopic) throws InterruptedException {
        long startTime = System.currentTimeMillis();

        int batchSize = 10;
        int i = 0;

        while (true) {
            if (i % batchSize == 0) {
                producer.flush();
                if (i == 200) {
                    break;
                }
            }
            String recordValue = toJson(i);
            sendMessage(producerTopic, recordValue);
            i++;
        }

        // This could be used for debugging purposes
        long endTime = System.currentTimeMillis();
		long elapsedTime = endTime - startTime;
		String recordValue = toJson(elapsedTime);

        sendMessage(producerTopic, recordValue);
		
		producer.flush();
		producer.close();
    }

    private static String toJson(int id) {
        return "{\"id\": " + id + ", \"name\": \"Apple\", \"price\": 7.77}";
    }

    private static String toJson(long elapsedTime) {
        return "{\"id\": " + 999 + ", \"name\": \"Apple\", \"price\":" + elapsedTime + "}";
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
}

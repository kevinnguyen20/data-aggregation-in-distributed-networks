package org.inet.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;

import java.util.Properties;

import java.util.ArrayList;
import java.util.List;

import org.inet.flink.model.Product;
import org.inet.flink.mapper.JsonToProductMapper;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	private static String KAFKA_BOOTSTRAP_SERVERS;
	private static String CONSUMER_TOPIC;
	
	static {
		Properties properties = new Properties();
		// try (FileInputStream fis = new FileInputStream("flink.properties")) {
		try (InputStream inputStream = DataStreamJob.class.getClassLoader().getResourceAsStream("flink.properties")) {
			properties.load(inputStream);

		} catch (IOException e) {
			e.getMessage();
		}
		KAFKA_BOOTSTRAP_SERVERS = properties.getProperty("KAFKA_BOOTSTRAP_SERVERS");
		CONSUMER_TOPIC = properties.getProperty("CONSUMER_TOPIC");
	}

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// env.setParallelism(8);

		KafkaSource<String> source = KafkaSource.<String>builder()
			.setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
			.setTopics(CONSUMER_TOPIC)
			.setGroupId("my-group")
			.setStartingOffsets(OffsetsInitializer.earliest())
			.setValueOnlyDeserializer(new SimpleStringSchema())
			.build();
		
		// Data generator
		dataGenerator(env);

		DataStream<String> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
		DataStream<Product> products = streamSource.map(new JsonToProductMapper());


		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.fromSequence(1, 10);
		 *
		 * then, transform the resulting DataStream<Long> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.window()
		 * 	.process()
		 *
		 * and many more.
		 * Have a look at the programming guide:
		 *
		 * https://nightlies.apache.org/flink/flink-docs-stable/
		 *
		 */

		products.print();
		
		// Execute program, beginning computation.
		env.execute("Flink Data Generation");
	}

	private static void dataGenerator(StreamExecutionEnvironment env) {
		Properties props = new Properties();

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		long startTime = System.currentTimeMillis();
		
		int batchSize = 10000000;
		for (int i = 1; i <= batchSize; i++) {
			String recordValue = "{\"id\": " + i + ", \"name\": \"Apple\", \"price\": 0.85}";
			ProducerRecord<String, String> record = new ProducerRecord<>(CONSUMER_TOPIC, recordValue);
			producer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e != null) {
						e.getMessage();
					} else {
						System.out.println("Produced record: " + recordValue);
					}
				}
			});
		}
		
		long endTime = System.currentTimeMillis();
		long elapsedTime = endTime - startTime;
		String recordValue = "{\"id\": " + 0 + ", \"name\": \"Apple\", \"price\":" + elapsedTime + "}";
		ProducerRecord<String, String> record = new ProducerRecord<>(CONSUMER_TOPIC, recordValue);
		producer.send(record, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception e) {
				if (e != null) {
					e.getMessage();
				} else {
					System.out.println("Produced record: " + recordValue);
				}
			}
		});

		// Print the elapsed time to the task manager's stdout
		
		
		producer.flush();
		producer.close();
	}

	private static String formatElapsedTime(long elapsedTime) {
		long seconds = elapsedTime / 1000;
		long milliseconds = elapsedTime % 1000;
		return String.format("%d.%03d seconds", seconds, milliseconds);
	}
}

package org.inet.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.io.IOException;

import java.util.Properties;

import org.inet.flink.model.Product;
import org.inet.flink.mapper.JsonToProductMapper;
import org.inet.flink.mapper.ProductToJsonMapper;
import org.inet.flink.generator.DataGenerator;

public class DataStreamJob {

	private static String KAFKA_BOOTSTRAP_SERVERS;
	private static String CONSUMER_TOPIC;
	private static String CLUSTER_COMMUNICATION_TOPIC;

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
			2, // Number of restart attempts
			Time.seconds(1L) // Delay between restarts
		));

		// Assigns values to the field variables
		loadProperties();
		
		// Starts data generation
		DataGenerator dataGenerator = new DataGenerator(KAFKA_BOOTSTRAP_SERVERS);
		dataGenerator.generateData(CONSUMER_TOPIC);

		// Receives data from data generator
		KafkaSource<String> dataGeneratorSource = createKafkaSource(CONSUMER_TOPIC);
		DataStream<String> streamSource = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "Kafka Data Generator");

		// Maps strings to product type
		// TODO do some data processing
		DataStream<Product> products = streamSource
			.map(new JsonToProductMapper())
			.filter(product -> product.getName().equals("Lemon"));
			
		products.print();

		DataStream<String> streamSink = products
			.map(new ProductToJsonMapper());

		// Starts transmitting data to the other cluster
		createLocalKafkaSink(streamSink);

		env.execute("Flink Data Generation");
	}

	private static void loadProperties() {
		Properties properties = new Properties();
		try (InputStream inputStream = DataStreamJob.class.getClassLoader().getResourceAsStream("flink.properties")) {
			properties.load(inputStream);
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		KAFKA_BOOTSTRAP_SERVERS = properties.getProperty("KAFKA_BOOTSTRAP_SERVERS");
        CONSUMER_TOPIC = properties.getProperty("CONSUMER_TOPIC");
		CLUSTER_COMMUNICATION_TOPIC = properties.getProperty("CLUSTER_COMMUNICATION_TOPIC");
	}

	/**
	 * Creates a local instance of Kafka source.
	 * @param topic a topic from which to receive data
	 * @return a Kafka source
	 */
	private static KafkaSource<String> createKafkaSource(String topic) {
		return KafkaSource.<String>builder()
				.setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
				.setTopics(topic)
				.setGroupId("data-generator")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();
	}

	/**
	 * Creates a local instance of Kafka sink and push data through it.
	 * @param productDataStream a stream which should be transmitted through the sink
	 */
	private static void createLocalKafkaSink(DataStream<String> productDataStream) {
		// TODO make it work with type Product to send the processed data
		KafkaSink<String> sink = KafkaSink.<String>builder()
				.setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(CLUSTER_COMMUNICATION_TOPIC)
						.setValueSerializationSchema(new SimpleStringSchema())
						.build()
				)
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();

		productDataStream.sinkTo(sink);
	}
}

package org.inet.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.inet.flink.generator.DataGenerator;
import org.inet.flink.mapper.JsonToProductMapper;
import org.inet.flink.model.Product;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DataStreamJob {

	private static String KAFKA_BOOTSTRAP_SERVERS;
	private static String CONSUMER_TOPIC_2;
	private static String CLUSTER_COMMUNICATION_TOPIC;

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
			2, // Number of restart attempts
			1000L // Delay between restarts
		));

		// Assigns values to the field variables
		loadProperties();

		// Starts data generation
		DataGenerator dataGenerator = new DataGenerator(KAFKA_BOOTSTRAP_SERVERS);
		dataGenerator.generateData(CONSUMER_TOPIC_2);

		// Receives data from data generator
		KafkaSource<String> dataGeneratorSource = createKafkaSource(CONSUMER_TOPIC_2, "data-generator");
		DataStream<String> streamSource = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "Kafka Data Generator");

		// Maps strings to product type
		DataStream<Product> products = streamSource
            .map(new JsonToProductMapper())
            .filter(product -> product.getName().equals("Apple"));

		DataStream<Double> price = products
			.map(Product::getPrice)
			.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
			.sum(0);

		products.print();
		price.print();

		// Starts receiving data from first cluster
		KafkaSource<String> firstClusterSource = createKafkaSource(CLUSTER_COMMUNICATION_TOPIC, "data-between-clusters");
		DataStream<String> dataFromFirstCluster = env.fromSource(firstClusterSource, WatermarkStrategy.noWatermarks(), "First Cluster Data");

		DataStream<Product> productsFromFirstCluster = dataFromFirstCluster
			.map(new JsonToProductMapper());

		DataStream<Double> priceForProductsFromFirstCluster = productsFromFirstCluster
			.map(Product::getPrice)
			.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
			.sum(0);

		productsFromFirstCluster.print();
		priceForProductsFromFirstCluster.print();

		env.execute("Flink Data Aggregation");
	}

	private static void loadProperties() {
		Properties properties = new Properties();
		try (InputStream inputStream = DataStreamJob.class.getClassLoader().getResourceAsStream("flink.properties")) {
			properties.load(inputStream);
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		KAFKA_BOOTSTRAP_SERVERS = properties.getProperty("KAFKA_BOOTSTRAP_SERVERS");
        CONSUMER_TOPIC_2 = properties.getProperty("CONSUMER_TOPIC_2");
		CLUSTER_COMMUNICATION_TOPIC = properties.getProperty("CLUSTER_COMMUNICATION_TOPIC");
	}

	/**
	 * Creates a local instance of Kafka source.
	 * @param topic a topic from which to receive data
	 * @param groupId id of a group (not important for our task)
	 * @return a Kafka source
	 */
	private static KafkaSource<String> createKafkaSource(String topic, String groupId) {
		return KafkaSource.<String>builder()
				.setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
				.setTopics(topic)
				.setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();
	}
}

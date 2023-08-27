package org.inet.flink;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;

import org.inet.flink.generator.DataGenerator;
import org.inet.flink.mapper.JsonToProductMapper;
import org.inet.flink.model.Product;
import org.inet.flink.mapper.LagCalculationFunction;

public class DataStreamJob {

	private static String KAFKA_BOOTSTRAP_SERVERS;
	private static String CONSUMER_TOPIC_2;
	private static String CLUSTER_COMMUNICATION_TOPIC;

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
			40, // Number of restart attempts
			1000L // Delay between restarts
		));
		// env.setParallelism(4);

		// Assigns values to the field variables
		loadProperties();

		// Uncomment to start the bounded data generator attached to the job
		// DataGenerator dataGenerator = new DataGenerator(KAFKA_BOOTSTRAP_SERVERS);
		// dataGenerator.generateData(CONSUMER_TOPIC_2);

		// Receive data from data generator
		KafkaSource<String> dataGeneratorSource = createKafkaSource(CONSUMER_TOPIC_2, "data-generator");
		WatermarkStrategy watermarkStrategy = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withIdleness(Duration.ofSeconds(15));
		DataStream<String> streamSource = env.fromSource(dataGeneratorSource,
		watermarkStrategy, "Kafka Data Generator");

		// Uncomment to use the file source (Kafka not needed)
		// DataStream<String> streamSource = env.readTextFile("../../../../../../../../records/output2.txt");

		// Map strings to product type and filters them by name and price
		DataStream<Product> products = streamSource
            .map(new JsonToProductMapper())
			.name("Map: Json to Product")
            .filter(product -> product.getName().equals("Apple") && product.getPrice()<0.8)
			.name("Filter: By Product name Apple and price less than 0.80 €");

		// Start receiving data from first cluster
		KafkaSource<String> firstClusterSource = createKafkaSource(CLUSTER_COMMUNICATION_TOPIC, "data-between-clusters");
		WatermarkStrategy watermarkStrategy2 = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withIdleness(Duration.ofSeconds(15));
		DataStream<String> dataFromFirstCluster = env.fromSource(firstClusterSource, watermarkStrategy2, "First Cluster Data");

		// Filter data from first cluster by name and price
		DataStream<Product> productsFromFirstCluster = dataFromFirstCluster
			.map(new JsonToProductMapper())
			.name("Map: Json to Product")
			.filter(product -> product.getName().equals("Lemon") && product.getPrice()<0.8)
			.name("Filter: By Product name Lemon and price less than 0.80 €");

		DataStream<Product> joinedProducts = products.union(productsFromFirstCluster);

		// Print number of products
		DataStream<String> countPerWindow = joinedProducts
			// .windowAll(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(5)))
			// .apply(new AllWindowFunction<Product, Long, TimeWindow>() {
			// 	public void apply(TimeWindow window, Iterable<Product> products, Collector<Long> out) throws Exception {
			// 		long count = 0;
			// 		for (Product product : products) {
			// 			count++;
			// 		}
			// 		out.collect(count);
			// 	}
			// })
			// .name("Apply: Counting products")
			// .map(count -> "Products: " + Math.round(count/30) + " records/s")
			// .name("Map: Formatted product count");
			// .process(new LagCalculationFunction())
    		// .name("Calculate Event Time Lag");
			.process(new ProcessFunction<Product, String>() {
				@Override
				public void processElement(Product product, Context context, Collector<String> collector) throws Exception {
					double currentTimestamp = (double) System.currentTimeMillis();
					double timeDifference = currentTimestamp - ( product.getTimestamp() * 1000); // Convert product timestamp to milliseconds
					
					String output = "Time Difference: " + timeDifference + " milliseconds";
					collector.collect(output);
				}
			})
			.name("Calculate Time Difference and Print")
			.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(5)))
			.apply(new AllWindowFunction<String, Double, TimeWindow>() {
				@Override
				public void apply(TimeWindow window, Iterable<String> input, Collector<Double> out) throws Exception {
					double sumTimeDifference = 0;
					int count = 0;
					
					for (String timeDifferenceStr : input) {
						double timeDifference = Double.parseDouble(timeDifferenceStr.split(": ")[1].split(" ")[0]); // Extract time difference value
						sumTimeDifference += timeDifference;
						count++;
					}
					
					double mean = sumTimeDifference / count;
					out.collect(mean);
				}
			})
			.name("Calculate Mean Time Difference")
			.map(mean -> "Time Difference: " + String.format("%.2f", mean) + " milliseconds");

			
		
		countPerWindow.print();

		// Print prices of products
		DataStream<String> sumOfPrices = joinedProducts
			.map(Product::getPrice)
			.name("Map: Extract prices")
			.windowAll(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(5)))
			.sum(0)
			.name("Sum: Over the prices")
			.map(sum -> (double) Math.round(sum/5*100)/100)
			.name("Map: Round to two decimal places")
			.map(price -> "Price: " + price + " €/s")
			.name("Map: Formatted price");

		// sumOfPrices.print();

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

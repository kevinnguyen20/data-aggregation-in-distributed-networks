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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.time.Duration;

public class DataStreamJob {

	private static String KAFKA_BOOTSTRAP_SERVERS;
	private static String CONSUMER_TOPIC_2;
	private static String CLUSTER_COMMUNICATION_TOPIC;

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
			20, // Number of restart attempts
			1000L // Delay between restarts
		));

		// Assigns values to the field variables
		loadProperties();

		// Uncomment to start the bounded data generator attached to the job
		// DataGenerator dataGenerator = new DataGenerator(KAFKA_BOOTSTRAP_SERVERS);
		// dataGenerator.generateData(CONSUMER_TOPIC_2);

		// Receives data from data generator
		KafkaSource<String> dataGeneratorSource = createKafkaSource(CONSUMER_TOPIC_2, "data-generator");
		DataStream<String> streamSource = env.fromSource(dataGeneratorSource,
		WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2)), "Kafka Data Generator");

		// Uncomment to use the file source (Kafka not needed)
		// DataStream<String> streamSource = env.readTextFile("../../../../../../../../records/output2.txt");

		// Maps strings to product type and filters them by name
		DataStream<Product> products = streamSource
            .map(new JsonToProductMapper())
			.name("Map: Json to Product")
            .filter(product -> product.getName().equals("Apple") && product.getPrice()<0.8)
			.name("Filter: By Product name Apple and price less than 0.80 €");

		// products.print();

		// Starts receiving data from first cluster
		KafkaSource<String> firstClusterSource = createKafkaSource(CLUSTER_COMMUNICATION_TOPIC, "data-between-clusters");
		DataStream<String> dataFromFirstCluster = env.fromSource(firstClusterSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2)), "First Cluster Data");

		// Filters data from first cluster by namea and price
		DataStream<Product> productsFromFirstCluster = dataFromFirstCluster
			.map(new JsonToProductMapper())
			.name("Map: Json to Product")
			.filter(product -> product.getName().equals("Lemon") && product.getPrice()<0.8)
			.name("Filter: By Product name Lemon and price less than 0.80 €");

		// productsFromFirstCluster.print();

		DataStream<Tuple2<Product, Product>> joinedProducts = products
			.connect(productsFromFirstCluster)
			// .name("Connect: product stream for the first cluster with the stream on the current cluster")
			.flatMap(new CoFlatMapFunction<Product, Product, Tuple2<Product, Product>>() {
				private Product lemonProduct;
				private Product appleProduct;

				@Override
				public void flatMap1(Product lemonProduct, Collector<Tuple2<Product, Product>> out) {
					this.lemonProduct = lemonProduct;
				}

				@Override
				public void flatMap2(Product appleProduct, Collector<Tuple2<Product, Product>> out) {
					this.appleProduct = appleProduct;
					if (lemonProduct!=null) {
						out.collect(new Tuple2<>(lemonProduct, appleProduct));
						lemonProduct = null;
						appleProduct = null;
					}
				}
			})
			.name("FlatMap: Create tuple of products from the first cluster with products on the current cluster");
			// .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
			// .apply(new AllWindowFunction<Tuple2<Product, Product>, Double, TimeWindow>() {
			// 	@Override
			// 	public void apply(TimeWindow window, Iterable<Tuple2<Product, Product>> products, Collector<Double> out) {
			// 		double sum = 0.0;
			// 		for (Tuple2<Product, Product> tuple : products) {
			// 			sum += tuple.f0.getPrice() + tuple.f1.getPrice();
			// 		}
			// 		out.collect(sum);
			// 	}
			// })
			// .name("Apply: Sum over prices")
			// .map(sum -> (double) Math.round(sum/10*100)/100)
			// .name("Map: Round to two decimal places")
			// .map(price -> "Total Price: " + price + " €/s")
			// .name("Map: Formatted total price");
			
		// Create a stream to calculate the sum over the prices from both streams
		// DataStream<Double> sumOfPricesStream = joinedProducts
		// 	.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(5)))
		// 	.apply(new AllWindowFunction<Tuple2<Product, Product>, Double, TimeWindow>() {
		// 		@Override
		// 		public void apply(TimeWindow window, Iterable<Tuple2<Product, Product>> products, Collector<Double> out) {
		// 			double sumOfPrices = 0.0;

		// 			for (Tuple2<Product, Product> tuple : products) {
		// 				if (tuple.f0 != null) {
		// 					sumOfPrices += tuple.f0.getPrice();
		// 				}
		// 				if (tuple.f1 != null) {
		// 					sumOfPrices += tuple.f1.getPrice();
		// 				}
		// 			}

		// 			out.collect(sumOfPrices);
		// 		}
		// 	})
		// 	.name("Apply: Calculate the sum over prices from both streams");

		// Print the sum of prices stream
		// sumOfPricesStream.print();

		// Create a stream to count the number of products from both streams
		DataStream<Tuple2<Integer, Integer>> countOfProductsStream = joinedProducts
			.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(5)))
			.apply(new AllWindowFunction<Tuple2<Product, Product>, Tuple2<Integer, Integer>, TimeWindow>() {
				@Override
				public void apply(TimeWindow window, Iterable<Tuple2<Product, Product>> products, Collector<Tuple2<Integer, Integer>> out) {
					int countProductsFromStream1 = 0;
					int countProductsFromStream2 = 0;

					for (Tuple2<Product, Product> tuple : products) {
						if (tuple.f0 != null) {
							countProductsFromStream1++;
						}
						if (tuple.f1 != null) {
							countProductsFromStream2++;
						}
					}

					out.collect(new Tuple2<>(countProductsFromStream1, countProductsFromStream2));
				}
			})
			.name("Apply: Count the number of products from both streams");

		// Print the count of products stream
		countOfProductsStream.print();

		// joinedProducts.print();

		// Sum over the prices of joined products
		// DataStream<Double> sumOfPrices = joinedProducts
		// 	.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
		// 	.apply(new AllWindowFunction<Tuple2<Product, Product>, Double, TimeWindow>() {
		// 		@Override
		// 		public void apply(TimeWindow window, Iterable<Tuple2<Product, Product>> products, Collector<Double> out) {
		// 			double sum = 0.0;
		// 			for (Tuple2<Product, Product> tuple : products) {
		// 				sum += tuple.f0.getPrice() + tuple.f1.getPrice();
		// 			}
		// 			out.collect(sum);
		// 		}
		// 	})
		// 	.name("Apply: Sum over prices");

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

package org.inet.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.NetUtils;

import org.apache.commons.io.IOUtils;

import java.io.EOFException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;

import org.inet.flink.model.Product;
import org.inet.flink.mapper.JsonToProductMapper;
import org.inet.flink.generator.DataGenerator;

public class DataStreamJob {

	private static String KAFKA_BOOTSTRAP_SERVERS;
	private static String CONSUMER_TOPIC_2;

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// env.setParallelism(8);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
			2, // Number of restart attempts
			Time.seconds(1L) // Delay between restarts
		));

		loadProperties();

		KafkaSource<String> source = createKafkaSource();
		
		// Data generator
		DataGenerator dataGenerator = new DataGenerator(KAFKA_BOOTSTRAP_SERVERS);
		String producerTopic = CONSUMER_TOPIC_2;
		dataGenerator.generateData(producerTopic);

		DataStream<String> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
		DataStream<Product> products = streamSource.map(new JsonToProductMapper());

		DataStream<String> result = null;
		try {
			// SocketTextStreamFunction socketSource = new SocketTextStreamFunction("localhost", 9981, "\n", 0);
			// result = env.socketTextStream("localhost", 9981, "\n", 0);
			// result = env.addSource(socketSource,
			// WatermarkStrategy.noWatermarks(), "Cluster Source");
			result = env.socketTextStream("localhost", 9981);
		} catch (Throwable t) {
            t.getMessage();
        }

		// String hostname = "localhost";
		// int port = 7777;
		// DataStream<String> socketSource = env.socketTextStream(hostname, port);
		// socketSource.addSink(new PrintSinkFunction<>());

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

		// products.print();
		result.print();
		env.execute("Flink Data Generation");
	}

	private static void loadProperties() {
		Properties properties = new Properties();
		try (InputStream inputStream = DataStreamJob.class.getClassLoader().getResourceAsStream("flink.properties")) {
			properties.load(inputStream);
		} catch (IOException e) {
			e.getMessage();
		}
		KAFKA_BOOTSTRAP_SERVERS = properties.getProperty("KAFKA_BOOTSTRAP_SERVERS");
        CONSUMER_TOPIC_2 = properties.getProperty("CONSUMER_TOPIC_2");
	}

	private static KafkaSource<String> createKafkaSource() {
		return KafkaSource.<String>builder()
				.setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
				.setTopics(CONSUMER_TOPIC_2)
				.setGroupId("my-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();
	}
}

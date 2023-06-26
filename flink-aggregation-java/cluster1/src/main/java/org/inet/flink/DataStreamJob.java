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

import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.NetUtils;

import org.apache.commons.io.IOUtils;

import java.net.*;
import java.io.*;

import org.apache.flink.streaming.api.datastream.DataStreamSource;

import org.inet.flink.model.Product;
import org.inet.flink.mapper.JsonToProductMapper;
import org.inet.flink.generator.DataGenerator;
import org.inet.flink.server.ServerThread;

public class DataStreamJob {

	private static String KAFKA_BOOTSTRAP_SERVERS;
	private static String CONSUMER_TOPIC;

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
		dataGenerator.generateData(CONSUMER_TOPIC);

		DataStream<String> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
		DataStream<Product> products = streamSource.map(new JsonToProductMapper());

		// SocketClientSink<String> socketSink = sendDataViaSocket();
		// SerializationSchema<String> simpleSchema =
        //     new SerializationSchema<String>() {
        //         @Override
        //         public byte[] serialize(String element) {
        //             return element.getBytes(ConfigConstants.DEFAULT_CHARSET);
        //         }
        //     };

		// DataStream<String> sinkSocket = null;

		// try {
		// 	SocketClientSink<String> simpleSink =
		// 			new SocketClientSink<>("localhost", 9981, simpleSchema, 0);
		// 	simpleSink.open(new Configuration());
		// 	simpleSink.invoke("Hello World" + '\n', SinkContextUtil.forTimestamp(0));
		// 	// simpleSink.close();
		// 	sinkSocket = env.addSink(simpleSink, WatermarkStrategy.noWatermarks(), "Hello World Source");
        // } catch (Throwable t) {
        //     t.getMessage();
        // }

		ServerThread serverThread = new ServerThread();
		serverThread.start();

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
        CONSUMER_TOPIC = properties.getProperty("CONSUMER_TOPIC");
	}

	private static KafkaSource<String> createKafkaSource() {
		return KafkaSource.<String>builder()
				.setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
				.setTopics(CONSUMER_TOPIC)
				.setGroupId("my-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();
	}

	// private static SocketClientSink<String> sendDataViaSocket() {
	// 	return new SocketClientSink<>("localhost", 7777,
	// 		new SocketWriter<String>() {
	// 			@Override
	// 			public void write(String value, OutputStream outputStream) throws IOException {
	// 				outputStream.write((value + "\n").getBytes());
	// 			}

	// 			@Override
	// 			public void close() throws IOException {}
	// 		});

	// }
}

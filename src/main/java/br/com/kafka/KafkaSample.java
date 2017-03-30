package br.com.kafka;

import static java.lang.String.join;
import static java.util.Arrays.asList;
import static java.util.Collections.reverse;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.apache.kafka.common.serialization.Serdes.String;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

/**
 * ./bin/zookeeper-server-start.sh config/zookeeper.properties
 * ./bin/kafka-server-start.sh config/server.properties
 * ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic simple-app
 * ./bin/kafka-topics.sh --list --zookeeper localhost:2181
 */
public class KafkaSample {

	private static final String TEST_TOPIC = "test";
	private static final String OUTPUT_TOPIC = "testOut";

	public static void main(final String... args) throws IOException {
		final ClassLoader loader = KafkaSample.class.getClassLoader();

		/*
		 * Producer configuration
		 */
		final Properties producerProps = new Properties();
		producerProps.load(loader.getResourceAsStream("kafka-producer.properties"));
		final SimpleProducer producer = new SimpleProducer(TEST_TOPIC, producerProps);

		/*
		 * First consumer configuration
		 */
		final Properties consumerProps = new Properties();
		consumerProps.load(loader.getResourceAsStream("kafka-consumer.properties"));
		final SimpleConsumer consumer1 = new SimpleConsumer(TEST_TOPIC, consumerProps, "Consumer1");

		/*
		 * Second consumer configuration
		 */
		consumerProps.setProperty("group.id", "simple-consumer1");
		final SimpleConsumer consumer2 = new SimpleConsumer(TEST_TOPIC, consumerProps, "Consumer2");

		final ExecutorService executorService = newFixedThreadPool(10);
		executorService.submit(producer::execute);
		executorService.submit(consumer1::execute);
		executorService.submit(consumer2::execute);
		executorService.submit(new SimpleConsumer(OUTPUT_TOPIC, consumerProps, "Consumer3")::execute);
		executorService.shutdown();

		/*
		 * Hight Level Stream API
		 */
		final Properties streamProps = new Properties();
		streamProps.load(loader.getResourceAsStream("kafka-stream.properties"));
		final StreamsConfig streamsConfig = new StreamsConfig(streamProps);

		final KStreamBuilder builder = new KStreamBuilder();
		builder.stream(String(), String(), TEST_TOPIC)
				.mapValues(value -> {
					final String[] quotes = value.split("");
					final List<String> reversed = new ArrayList<>(asList(quotes));
					reverse(reversed);
					return join("", reversed);
				}).to(OUTPUT_TOPIC);

		final KafkaStreams streams = new KafkaStreams(builder, streamsConfig);
		streams.start();
	}
}

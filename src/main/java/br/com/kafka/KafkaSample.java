package br.com.kafka;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * ./bin/zookeeper-server-start.sh config/zookeeper.properties
 * ./bin/kafka-server-start.sh config/server.properties
 * ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic simple-app
 * ./bin/kafka-topics.sh --list --zookeeper localhost:2181
 * <p>
 * https://www.confluent.io/blog/introducing-kafka-streams-stream-processing-made-simple/
 */
public class KafkaSample {

	public static void main(final String... args) throws IOException {
		if (args.length == 0) {
			throw new IllegalStateException("Topic name was not supplied!");
		}

		final ClassLoader loader = KafkaSample.class.getClassLoader();

		final Properties producerProps = new Properties();
		producerProps.load(loader.getResourceAsStream("kafka-producer.properties"));
		final SimpleProducer producer = new SimpleProducer(args[0], producerProps);

		final Properties consumerProps = new Properties();
		consumerProps.load(loader.getResourceAsStream("kafka-consumer.properties"));
		final SimpleConsumer consumer1 = new SimpleConsumer(args[0], consumerProps, "Consumer1");

		consumerProps.setProperty("group.id", "simple-consumer1");
		final SimpleConsumer consumer2 = new SimpleConsumer(args[0], consumerProps, "Consumer2");

		final ExecutorService executorService = Executors.newFixedThreadPool(10);
		executorService.submit(producer::execute);
		executorService.submit(consumer1::execute);
		executorService.submit(consumer2::execute);
		executorService.shutdown();
	}
}

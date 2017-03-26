package br.com.kafka;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer implements KafkaExecutor {

	private final String topicName;
	private final Producer<String, String> producer;

	public SimpleProducer(final String topicName, final Properties props) {
		this.topicName = topicName;
		this.producer = new KafkaProducer<>(props);
	}

	public void execute() {
		try (final Scanner scan = new Scanner(System.in)) {
			int sequence = 0;
			while (true) {
				final String message = scan.nextLine();
				final String key = "key_" + sequence++;

				if ("exit".equalsIgnoreCase(message)) {
					break;
				}

				final ProducerRecord<String, String> record = new ProducerRecord<>(this.topicName, key, message);
				this.producer.send(record);
			}
		}
	}
}

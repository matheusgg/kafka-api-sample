package br.com.kafka;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimpleConsumer implements KafkaExecutor {

	private final String topicName;
	private final Consumer<String, String> consumer;
	private final String consumerName;

	public SimpleConsumer(final String topicName, final Properties props, final String consumerName) {
		this.topicName = topicName;
		this.consumer = new KafkaConsumer<>(props);
		this.consumerName = consumerName;
	}

	@Override
	public void execute() {
		this.consumer.subscribe(singletonList(this.topicName));
		while (true) {
			final ConsumerRecords<String, String> records = this.consumer.poll(100);
			for (final ConsumerRecord<String, String> record : records) {
				System.out.println(format("%s ==> offset=%d, key=%s, value=%s", this.consumerName, record.offset(), record.key(), record.value()));
			}
		}
	}
}

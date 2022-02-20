package br.com.kafka.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public final class ConsumerFactory {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerFactory.class);

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TOPIC = "first-topic";

    public static void create(String groupId) {
        var properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, groupId);
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

        var consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton(TOPIC));

        while (true) {
            var consumerRecords = consumer.poll(Duration.ofMillis(100));
            consumerRecords.forEach(consumerRecord ->
                    logger.info(String.format("DateTime: %s - Key: %s - Value: %s - Partition: %s - Offset: %s",
                            LocalDateTime.now(), consumerRecord.key(), consumerRecord.value(), consumerRecord.partition(), consumerRecord.offset())));
        }
    }

}

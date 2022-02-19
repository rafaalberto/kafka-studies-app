package br.com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class Producer {

    private final Logger logger = LoggerFactory.getLogger(Producer.class);

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TOPIC = "first-topic";

    public void create(String message) {
        var properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        var producerRecord = new ProducerRecord<String, String>(TOPIC, message);

        var producer = new KafkaProducer<String, String>(properties);
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                logger.info(String.format("Received metadata - Topic: %s - Partition: %s - Offset: %s",
                        metadata.topic(), metadata.partition(), metadata.offset()));
            } else {
                logger.error("Error while producing: ", exception);
            }
        });
        producer.flush();
        producer.close();
    }

}

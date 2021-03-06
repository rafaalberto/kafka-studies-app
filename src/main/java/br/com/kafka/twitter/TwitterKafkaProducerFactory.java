package br.com.kafka.twitter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public final class TwitterKafkaProducerFactory {

    private static final Logger logger = LoggerFactory.getLogger(TwitterKafkaProducerFactory.class);

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String TOPIC = "twitter-tweets";

    private final KafkaProducer<String, String> kafkaProducer;

    public TwitterKafkaProducerFactory() {
        kafkaProducer = new KafkaProducer<>(getProperties());
    }

    public void sendMessage(final String message) {
        var producerRecord = new ProducerRecord<String, String>(TOPIC, message);
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                logger.info(String.format("Sending metadata - DateTime: %s - Topic: %s - Partition: %s - Offset: %s",
                        LocalDateTime.now(), metadata.topic(), metadata.partition(), metadata.offset()));
            } else {
                logger.error("Error while producing: ", exception);
            }
        });
    }

    public void close() {
        kafkaProducer.close();
    }

    private Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        setSaferSettings(properties);
        setHighThroughputSettings(properties);
        return properties;
    }

    private void setSaferSettings(Properties properties) {
        properties.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ACKS_CONFIG, "all");
        properties.setProperty(RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    }

    private void setHighThroughputSettings(Properties properties) {
        properties.setProperty(COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(LINGER_MS_CONFIG, "20");
        properties.setProperty(BATCH_SIZE_CONFIG, "32768");
    }

}

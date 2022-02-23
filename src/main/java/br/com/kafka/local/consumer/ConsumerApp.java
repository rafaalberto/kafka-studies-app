package br.com.kafka.local.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerApp {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerApp.class);

    public static void main(String[] args) {
        logger.info("Consumer has been started");
        ConsumerFactory.create("my-app-1");
    }

}

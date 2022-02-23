package br.com.kafka.local.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerApp {

    private static final Logger logger = LoggerFactory.getLogger(ProducerApp.class);

    public static void main(String[] args) {
        logger.info("Producer has been started");
        ProducerFactory.create("Send and get by app");
    }

}

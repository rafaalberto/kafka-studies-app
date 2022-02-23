package br.com.kafka.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterApp {

    private static final Logger logger = LoggerFactory.getLogger(TwitterApp.class);

    public static void main(String[] args) {
        logger.info("Starting Twitter Fetch Messages!");
        var messageQueue = new LinkedBlockingQueue<String>(1000);
        var client = TwitterClient.createClient(messageQueue);
        client.connect();

        while (!client.isDone()) {
            try {
                var message = messageQueue.poll(5, TimeUnit.SECONDS);
                logger.info("Message from twitter: {}", message);
            } catch (InterruptedException e) {
                logger.error("Error to poll messages: {}", e.getMessage());
                client.stop();
                Thread.currentThread().interrupt();
            }
        }

    }

}

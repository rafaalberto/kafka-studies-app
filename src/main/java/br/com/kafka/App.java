package br.com.kafka;

import br.com.kafka.producer.Producer;

public class App {

    public static void main(String[] args) {
        new Producer().create("Testing producer 4!");
    }

}

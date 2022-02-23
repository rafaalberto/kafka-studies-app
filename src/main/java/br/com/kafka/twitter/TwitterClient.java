package br.com.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public final class TwitterClient {

    private static final String CONSUMER_KEY = "7xYD81gfO3VHIkcHGONgjxKCf";
    private static final String CONSUMER_SECRET = "Coxo0vg7tSImpsOZpULsrPWEAfvG1RwQHq7ctIA1va8D5Qv4ha";
    private static final String TOKEN = "1496462003107422210-bnKNQtiAPVQaH5AH0gkNMI5174lB4S";
    private static final String SECRET = "H0vZO3J6j18YPAFSqhQI52zDmfYrDcmzyJKz1kq85yTjH";

    public static Client createClient(BlockingQueue<String> messageQueue) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("java");
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(messageQueue));

        return builder.build();
    }

}

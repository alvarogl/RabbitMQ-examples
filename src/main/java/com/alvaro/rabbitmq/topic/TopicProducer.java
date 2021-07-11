package com.alvaro.rabbitmq.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicProducer {

    private final static Logger LOGGER = LoggerFactory.getLogger(TopicProducer.class);
    private static final String EXCHANGE_NAME = "sport-events";

    public static void main(String[] args) {
        //Open connection and set channel
        ConnectionFactory factory = new ConnectionFactory();

        try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {
            //Create fanout exchange
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

            List<String> countries = Arrays.asList("es", "fr", "uk");
            List<String> sports = Arrays.asList("football", "tennis", "volleyball");
            List<String> eventTypes = Arrays.asList("live", "news");
            int counter = 0;
            while (true) {
                shuffle(countries, sports, eventTypes);
                String country = countries.get(0);
                String sport = sports.get(0);
                String type = eventTypes.get(0);

                String routingKey = country + "." + sport + "." + type;

                String message = "Evento-" + counter;
                LOGGER.info("Producing message (" + routingKey + "): " + message);
                channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
                counter++;
                Thread.sleep(1000);
            }

        } catch (IOException | TimeoutException | InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }

    }

    private static void shuffle(List<String> countries, List<String> sports,
        List<String> eventTypes) {
        Collections.shuffle(countries);
        Collections.shuffle(sports);
        Collections.shuffle(eventTypes);
    }
}

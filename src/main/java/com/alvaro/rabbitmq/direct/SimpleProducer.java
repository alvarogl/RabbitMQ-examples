package com.alvaro.rabbitmq.direct;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducer {

    private final static Logger LOGGER = LoggerFactory.getLogger(SimpleProducer.class);
    private static final String QUEUE_NAME = "first-queue";

    public static void main(String[] args) {
        final String message = "Hello, world!";

        //Open connection
        ConnectionFactory factory = new ConnectionFactory();
        try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {
            //Establish channel
            connection.createChannel();

            //Create queue
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            //Publish message
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());

        } catch (IOException | TimeoutException e) {
            LOGGER.error(e.getMessage(), e);
        }

    }
}

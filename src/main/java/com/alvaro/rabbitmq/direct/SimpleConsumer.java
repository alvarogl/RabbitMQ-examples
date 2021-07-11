package com.alvaro.rabbitmq.direct;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumer.class);
    private static final String QUEUE_NAME = "first-queue";

    public static void main(String[] args) {
        //Open Connection
        //Establish channel
        ConnectionFactory factory = new ConnectionFactory();
        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            //Subscribe to "first-queue"
            channel.basicConsume(QUEUE_NAME, true,
                (consumerTag, message) -> {
                    String messageBody = new String(message.getBody());
                    LOGGER.info("Consumer: " + consumerTag);
                    LOGGER.info("Message: " + messageBody);
                    LOGGER.info("Exchange: " + message.getEnvelope().getExchange());
                    LOGGER.info("Routing key: " + message.getEnvelope().getRoutingKey());
                },
                consumerTag -> {
                    LOGGER.info("Consumer " + consumerTag + " cancelled");
                });
        } catch (IOException | TimeoutException e) {
            LOGGER.error(e.getMessage(), e);
        }

    }
}

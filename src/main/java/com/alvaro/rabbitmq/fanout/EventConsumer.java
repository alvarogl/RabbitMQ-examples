package com.alvaro.rabbitmq.fanout;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventConsumer.class);
    private static final String EXCHANGE_NAME = "events";

    public static void main(String[] args) {
        //Open Connection
        //Establish channel
        ConnectionFactory factory = new ConnectionFactory();
        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

            //Create queue associated to exchange "events"
            String queueNameUnique = channel.queueDeclare().getQueue();
            channel.queueBind(queueNameUnique, EXCHANGE_NAME, "");

            channel.basicConsume(queueNameUnique, true,
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

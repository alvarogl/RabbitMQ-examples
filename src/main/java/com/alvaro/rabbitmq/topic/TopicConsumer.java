package com.alvaro.rabbitmq.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicConsumer.class);
    private static final String EXCHANGE_NAME = "sport-events";

    public static void main(String[] args) {
        //Open Connection
        //Establish channel
        ConnectionFactory factory = new ConnectionFactory();
        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

            //Create queue associated to exchange "events"
            String queueNameUnique = channel.queueDeclare().getQueue();

            System.out.println("Input routing-key: ");
            Scanner scanner = new Scanner(System.in);
            String routingKey = scanner.nextLine();
            channel.queueBind(queueNameUnique, EXCHANGE_NAME, routingKey);

            channel.basicConsume(queueNameUnique, true,
                (consumerTag, message) -> {
                    String messageBody = new String(message.getBody());
                    LOGGER.info("Consumer: " + consumerTag);
                    LOGGER.info("Message: " + messageBody);
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

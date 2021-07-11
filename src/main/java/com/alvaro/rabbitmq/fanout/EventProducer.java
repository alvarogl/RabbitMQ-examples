package com.alvaro.rabbitmq.fanout;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventProducer {

    private final static Logger LOGGER = LoggerFactory.getLogger(EventProducer.class);
    private static final String QUEUE_NAME = "events";

    public static void main(String[] args) {
        //Open connection and set channel
        ConnectionFactory factory = new ConnectionFactory();

        try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {
            //Create fanout exchange
            channel.exchangeDeclare(QUEUE_NAME, BuiltinExchangeType.FANOUT);

            //Send messages to fanout exchange "events"
            int counter = 0;
            while(true) {
                String message = "Evento-" + counter;
                LOGGER.info("Producing message " + message);
                channel.basicPublish(QUEUE_NAME, "", null, message.getBytes());
                counter++;
                Thread.sleep(1000);
            }

        } catch (IOException | TimeoutException | InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }

    }
}

package com.elon.consumer.service;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMq消费者服务类
 *
 * @author elon
 * @since 2022-02-02
 */
public class RabbitMqConsumerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqConsumerService.class);

    private final static String QUEUE_NAME = "elon_queue";

    private final static String EXCHANGE_NAME = "elon_exchange";

    /**
     * 消费消息
     */
    public void consumeMessage() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.43.134");
        factory.setPort(5672);
        factory.setUsername("yzy");
        factory.setPassword("yzy614114");

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                LOGGER.info("Receive message from queue:{}", message);
            };

            channel.basicConsume(QUEUE_NAME, true, deliverCallback, tag->{});
        } catch (Exception e) {
            LOGGER.error("Consume message exception.", e);
        }
    }

    /**
     * 消费交换器转发的消息
     */
    public void consumeMessageFromExchange(){
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.43.134");
        factory.setPort(5672);
        factory.setUsername("yzy");
        factory.setPassword("yzy614114");

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            
            String queueName = channel.queueDeclare().getQueue();
            LOGGER.info("Get queue name:{}", queueName);

            channel.queueBind(queueName, EXCHANGE_NAME, "");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                LOGGER.info("Receive message from binding exchange:{}", message);
            };

            channel.basicConsume(queueName, true, deliverCallback, tag->{});
        } catch (Exception e) {
            LOGGER.error("Consume message exception.", e);
        }
    }
}

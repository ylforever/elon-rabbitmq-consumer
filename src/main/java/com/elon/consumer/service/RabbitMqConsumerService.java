package com.elon.consumer.service;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * RabbitMq消费者服务类
 *
 * @author elon
 * @since 2022-02-02
 */
public class RabbitMqConsumerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqConsumerService.class);

    private final static String QUEUE_NAME = "elon_queue";

    private final static String EXCHANGE_FANOUT_NAME = "exchange_fanout_name";

    private final static String EXCHANGE_TOPIC_NAME = "exchange_topic_name";

    /**
     * 消费消息
     */
    public void consumeMessage() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.5.128");
        factory.setPort(5672);
        factory.setUsername("yzy");
        factory.setPassword("yzy614114");

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            // 一次只预取一个消息处理
            int prefetchCount = 1;
            channel.basicQos(prefetchCount);

            // 不自动确认，在消费完成后主动确认.
            boolean autoAck = false;
            channel.basicConsume(QUEUE_NAME, autoAck, new MQMessageHandler(channel), tag -> {
            });
        } catch (Exception e) {
            LOGGER.error("Consume message exception.", e);
        }
    }

    /**
     * 消费交换器转发的消息
     */
    public void consumeMessageFromFanoutExchange() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.5.128");
        factory.setPort(5672);
        factory.setUsername("yzy");
        factory.setPassword("yzy614114");

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_FANOUT_NAME, "fanout");

            String queueName = channel.queueDeclare().getQueue();
            LOGGER.info("Get queue name:{}", queueName);
            channel.queueBind(queueName, EXCHANGE_FANOUT_NAME, "");
            channel.basicConsume(queueName, false, new MQMessageHandler(channel), tag -> {
            });
        } catch (Exception e) {
            LOGGER.error("Consume message exception.", e);
        }
    }

    public void consumeMessageFromTopicExchange(List<String> bindKeys) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.5.128");
        factory.setPort(5672);
        factory.setUsername("yzy");
        factory.setPassword("yzy614114");

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_TOPIC_NAME, "topic");
            String queueName = channel.queueDeclare().getQueue();

            for (String bindKey : bindKeys) {
                channel.queueBind(queueName, EXCHANGE_TOPIC_NAME, bindKey);
            }

            channel.basicConsume(queueName, false, new MQMessageHandler(channel), tag -> {
            });
        } catch (Exception e) {
            LOGGER.error("Consume message exception.", e);
        }
    }
}

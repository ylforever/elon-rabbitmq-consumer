package com.elon.consumer.service;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * MQ消息消费处理类
 *
 * @author elon
 * @since 2022-03-06
 */
public class MQMessageHandler implements DeliverCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(MQMessageHandler.class);

    private Channel channel;

    public MQMessageHandler(Channel channel) {
        this.channel = channel;
    }

    @Override
    public void handle(String s, Delivery delivery) throws IOException {
        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        LOGGER.info("Receive message:{}", message);
        try {
            Thread.sleep(1 * 1000);
            long tag = delivery.getEnvelope().getDeliveryTag();

            // 处理完任务后再主动确认
            channel.basicAck(tag, false);
        } catch (InterruptedException e) {
            LOGGER.error("Handle error.", e);
        }
    }
}

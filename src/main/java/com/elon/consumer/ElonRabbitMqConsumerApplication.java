package com.elon.consumer;

import com.elon.consumer.service.RabbitMqConsumerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

/**
 * RabbitMQ 消费者引用启动类
 *
 * @author elon
 * @since 2022-01-19
 */
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class ElonRabbitMqConsumerApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElonRabbitMqConsumerApplication.class);
    public static void main(String[] args) {
        SpringApplication.run(ElonRabbitMqConsumerApplication.class);
        RabbitMqConsumerService consumerService = new RabbitMqConsumerService();
//        consumerService.consumeMessage();
        consumerService.consumeMessageFromExchange();
        LOGGER.info("Start up rabbitmq consumer success.");
    }
}

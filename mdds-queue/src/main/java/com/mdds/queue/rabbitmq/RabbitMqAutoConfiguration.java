/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableRabbit
@EnableConfigurationProperties(RabbitMqProperties.class)
public class RabbitMqAutoConfiguration {

  @Bean(destroyMethod = "destroy")
  public CachingConnectionFactory rabbitConnectionFactory(RabbitMqProperties properties) {
    var ccf = new CachingConnectionFactory(properties.getHost(), properties.getPort());
    ccf.setUsername(properties.getUser());
    ccf.setPassword(properties.getPassword());
    ccf.setPublisherReturns(true);
    return ccf;
  }
}

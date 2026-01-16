/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue;

import com.mdds.queue.rabbitmq.RabbitMqProperties;
import com.mdds.queue.rabbitmq.RabbitMqQueue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QueueConfig {

  @Bean(name = "taskQueue")
  public Queue taskQueue(RabbitMqProperties properties) {
    return new RabbitMqQueue(
        properties.getHost(),
        properties.getPort(),
        properties.getUser(),
        properties.getPassword(),
        properties.getMaxInboundMessageBodySize());
  }

  @Bean(name = "resultQueue")
  public Queue resultQueue(RabbitMqProperties properties) {
    return new RabbitMqQueue(
        properties.getHost(),
        properties.getPort(),
        properties.getUser(),
        properties.getPassword(),
        properties.getMaxInboundMessageBodySize());
  }

  @Bean(name = "cancelQueue")
  public Queue cancelQueue(RabbitMqProperties properties) {
    return new RabbitMqQueue(
        properties.getHost(),
        properties.getPort(),
        properties.getUser(),
        properties.getPassword(),
        properties.getMaxInboundMessageBodySize());
  }
}

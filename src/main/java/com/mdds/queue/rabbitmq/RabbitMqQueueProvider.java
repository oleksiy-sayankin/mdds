/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import com.mdds.queue.Queue;
import com.mdds.queue.QueueFactory;
import com.mdds.queue.QueueProvider;

/** Provider for RabbitMq. */
public class RabbitMqQueueProvider implements QueueProvider {
  /**
   * Creates RabbitMq configuration and an instance of RabbitMq connection.
   *
   * @return instance of RabbitMq connection.
   */
  @Override
  public Queue get() {
    return QueueFactory.createRabbitMq(RabbitMqConfFactory.fromEnvOrDefaultProperties());
  }
}

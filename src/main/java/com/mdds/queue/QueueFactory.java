/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue;

import com.mdds.queue.rabbitmq.RabbitMqConf;
import com.mdds.queue.rabbitmq.RabbitMqQueue;
import jakarta.annotation.Nonnull;

/** Basic factory for all queues. */
public final class QueueFactory {
  private QueueFactory() {}

  public static @Nonnull Queue createRabbitMq(
      @Nonnull String host, int port, String user, String password) {
    return new RabbitMqQueue(host, port, user, password);
  }

  public static @Nonnull Queue createRabbitMq(@Nonnull RabbitMqConf conf) {
    return new RabbitMqQueue(conf);
  }
}

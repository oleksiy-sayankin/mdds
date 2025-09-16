/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue;

import com.mdds.queue.rabbitmq.RabbitMqConf;
import com.mdds.queue.rabbitmq.RabbitMqQueue;

/** Basic factory for all queues. */
public final class QueueFactory {
  private QueueFactory() {}

  public static Queue createRabbitMq(String host, int port, String user, String password) {
    return new RabbitMqQueue(host, port, user, password);
  }

  public static Queue createRabbitMq(RabbitMqConf conf) {
    return new RabbitMqQueue(conf);
  }
}

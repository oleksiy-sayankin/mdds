/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import com.rabbitmq.client.AMQP;
import java.util.Map;

/** Utility method for RabbitMq. */
public final class RabbitMqHelper {
  private RabbitMqHelper() {}

  /**
   * Converts Map to AMQP.BasicProperties.
   *
   * @param headers Map with parameters.
   * @return Equivalent of the input map but as AMQP.BasicProperties
   */
  public static AMQP.BasicProperties convertFrom(Map<String, Object> headers) {
    return new AMQP.BasicProperties.Builder().headers(headers).build();
  }
}

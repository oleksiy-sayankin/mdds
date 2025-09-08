/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

/** Exception during connection try to RabbitMq. */
public class RabbitMqConnectionException extends RuntimeException {

  public RabbitMqConnectionException(String message, Throwable cause) {
    super(message, cause);
  }

  public RabbitMqConnectionException(String message) {
    super(message);
  }
}

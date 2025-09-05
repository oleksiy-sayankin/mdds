/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rebbitmq;

public class RabbitMqConnectionException extends RuntimeException {
  public RabbitMqConnectionException(Throwable cause) {
    super(cause);
  }

  public RabbitMqConnectionException() {
    super();
  }
}

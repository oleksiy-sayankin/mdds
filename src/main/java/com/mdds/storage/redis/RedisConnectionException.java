/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

/** Thrown when there is no connection to Redis. */
public class RedisConnectionException extends RuntimeException {
  public RedisConnectionException(Throwable cause) {
    super(cause);
  }

  public RedisConnectionException() {
    super();
  }
}

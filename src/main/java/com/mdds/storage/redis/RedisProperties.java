/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

/**
 * Base class for Redis connection properties.
 *
 * @param host Redis host
 * @param port Redis port
 */
public record RedisProperties(String host, int port) {
  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 6379;
}

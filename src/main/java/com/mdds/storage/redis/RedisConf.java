/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

import com.google.common.annotations.VisibleForTesting;

import static com.mdds.common.util.CommonHelper.readPropertiesOrEmpty;
import static com.mdds.common.util.ConfigResolution.resolveInt;
import static com.mdds.common.util.ConfigResolution.resolveString;

/**
 * Base class for Redis connection properties.
 *
 * @param host Redis host
 * @param port Redis port
 */
public record RedisConf(String host, int port) {
  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 6379;
  public static final String REDIS_DEFAULT_CONF_FILE_NAME = "redis.properties";

  @VisibleForTesting
  public static RedisConf fromEnvOrProperties(String fileName) {
    var props = readPropertiesOrEmpty(fileName);
    var host = resolveString("redis.host", "REDIS_HOST", props, DEFAULT_HOST);
    var port = resolveInt("redis.port", "REDIS_PORT", props, DEFAULT_PORT);
    return new RedisConf(host, port);
  }

  public static RedisConf fromEnvOrDefaultProperties() {
    return fromEnvOrProperties(REDIS_DEFAULT_CONF_FILE_NAME);
  }
}

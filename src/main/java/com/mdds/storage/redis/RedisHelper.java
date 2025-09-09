/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

import com.mdds.queue.rabbitmq.RabbitMqHelper;
import java.io.IOException;
import java.util.Properties;

/** Utility class for Redis. */
public final class RedisHelper {
  private RedisHelper() {}

  /**
   * Reads Redis connection parameters from properties file in classpath.
   *
   * @param redisProperties *.properties file in classpath.
   * @return record with connection parameters
   */
  public static RedisProperties readFromFile(String redisProperties) {
    var properties = new Properties();
    try (var input = RabbitMqHelper.class.getClassLoader().getResourceAsStream(redisProperties)) {
      if (input == null) {
        throw new RedisConnectionException(redisProperties + " not found in resources");
      }
      properties.load(input);
    } catch (IOException e) {
      throw new RedisConnectionException("Could not load file " + redisProperties, e);
    }
    var host =
        System.getProperty(
            "redis.host", properties.getProperty("redis.host", RedisProperties.DEFAULT_HOST));
    int port =
        Integer.parseInt(
            System.getProperty(
                "redis.port",
                properties.getProperty(
                    "redis.port", String.valueOf(RedisProperties.DEFAULT_PORT))));
    return new RedisProperties(host, port);
  }
}

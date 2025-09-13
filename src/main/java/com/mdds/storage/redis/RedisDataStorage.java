/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

import com.mdds.storage.DataStorage;
import com.mdds.util.JsonHelper;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/** Data storage that stores data in Redis key-value DB. */
public class RedisDataStorage implements DataStorage {
  private UnifiedJedis jedis;
  private static final Logger LOGGER = LoggerFactory.getLogger(RedisDataStorage.class);

  public RedisDataStorage(RedisProperties properties) {
    connect(properties.host(), properties.port());
  }

  public RedisDataStorage(String host, int port) {
    connect(host, port);
  }

  @Override
  public <T> void put(String key, T value) {
    jedis.set(key, JsonHelper.toJson(value));
  }

  @Override
  public <T> Optional<T> get(String key, Class<T> type) {
    var json = jedis.get(key);
    if (json == null) {
      return Optional.empty();
    }
    return Optional.of(JsonHelper.fromJson(json, type));
  }

  @Override
  public void close() {
    if (jedis != null) {
      jedis.close();
    }
  }

  private void connect(String host, int port) {
    try {
      jedis = new UnifiedJedis("redis://" + host + ":" + port);
      String result = jedis.ping();
      if ("PONG".equals(result)) {
        LOGGER.info("Connected to Redis redis://{}:{}", host, port);
      } else {
        throw new RedisConnectionException(
            "Redis connection redis://"
                + host
                + ":"
                + port
                + " failed with unexpected response: "
                + result);
      }
    } catch (JedisConnectionException e) {
      throw new RedisConnectionException("Failed to connect to redis://" + host + ":" + port, e);
    }
  }
}

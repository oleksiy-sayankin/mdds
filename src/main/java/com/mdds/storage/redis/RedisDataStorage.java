/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

import com.mdds.storage.DataStorage;
import com.mdds.util.JsonHelper;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/** Data storage that stores data in Redis key-value DB. */
@Slf4j
public class RedisDataStorage implements DataStorage {
  private final @Nonnull UnifiedJedis jedis;

  public RedisDataStorage(@Nonnull RedisConf conf) {
    this(conf.host(), conf.port());
  }

  public RedisDataStorage(@Nonnull String host, int port) {
    try {
      jedis = new UnifiedJedis("redis://" + host + ":" + port);
      var result = jedis.ping();
      if ("PONG".equals(result)) {
        log.info("Connected to Redis redis://{}:{}", host, port);
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
    jedis.close();
  }
}

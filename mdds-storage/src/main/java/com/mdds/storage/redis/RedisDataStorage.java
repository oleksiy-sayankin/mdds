/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

import com.google.common.annotations.VisibleForTesting;
import com.mdds.common.util.JsonHelper;
import com.mdds.storage.DataStorage;
import jakarta.annotation.Nonnull;
import java.time.Duration;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
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
    this(host, port, Duration.ofSeconds(60));
  }

  @VisibleForTesting
  public RedisDataStorage(@Nonnull RedisConf conf, Duration timeOut) {
    this(conf.host(), conf.port(), timeOut);
  }

  @VisibleForTesting
  public RedisDataStorage(@Nonnull String host, int port, Duration timeOut) {
    try {
      jedis = new UnifiedJedis("redis://" + host + ":" + port);
      untilRedisAvailable(jedis, host, port, timeOut);
      log.info("Connected to Redis redis://{}:{}", host, port);
    } catch (JedisConnectionException e) {
      throw new RedisConnectionException("Failed to connect to redis://" + host + ":" + port, e);
    }
  }

  private static void untilRedisAvailable(
      UnifiedJedis jedis, String host, int port, Duration timeOut) {
    try {
      Awaitility.await()
          .atMost(timeOut)
          .pollInterval(Duration.ofSeconds(1))
          .pollDelay(Duration.ZERO)
          .logging((s -> log.info("Connecting redis://{}:{}...", host, port)))
          .ignoreExceptions()
          .until(() -> "PONG".equals(jedis.ping()));
    } catch (ConditionTimeoutException e) {
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

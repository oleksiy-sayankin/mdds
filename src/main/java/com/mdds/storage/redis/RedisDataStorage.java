/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

import com.mdds.storage.DataStorage;
import com.mdds.util.JsonHelper;
import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.UnifiedJedis;

/** Data storage that stores data in Redis key-value DB. */
public class RedisDataStorage implements DataStorage {
  private UnifiedJedis jedis;
  private static final Logger LOGGER = LoggerFactory.getLogger(RedisDataStorage.class);

  private static final DataStorage DATA_STORAGE = new RedisDataStorage();

  private RedisDataStorage() {}

  public static DataStorage getInstance() {
    return DATA_STORAGE;
  }

  @Override
  public void connect() {
    var properties = new Properties();
    try (var input = getClass().getClassLoader().getResourceAsStream("redis.properties")) {
      if (input == null) {
        LOGGER.error("redis.properties not found in resources");
        throw new RedisConnectionException();
      }
      properties.load(input);
    } catch (IOException e) {
      LOGGER.error("Could not load redis.properties file.");
      throw new RedisConnectionException(e);
    }
    var host = System.getProperty("redis.host", properties.getProperty("redis.host", "localhost"));
    int port =
        Integer.parseInt(
            System.getProperty("redis.port", properties.getProperty("redis.port", "6379")));
    jedis = new UnifiedJedis("redis://" + host + ":" + port);
    LOGGER.info("Connected to Redis redis://{}:{}", host, port);
  }

  @Override
  public void put(String key, Object value) {
    jedis.set(key, JsonHelper.toJson(value));
  }

  @Override
  public <T> T get(String key, Class<T> type) {
    var json = jedis.get(key);
    if (json == null) {
      return null;
    }
    return JsonHelper.fromJson(json, type);
  }

  @Override
  public void close() {
    if (jedis != null) {
      jedis.close();
    }
  }
}

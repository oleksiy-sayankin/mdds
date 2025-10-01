/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

import static com.mdds.storage.redis.RedisHelper.readFromResources;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestRedisHelper {

  @Test
  void testNoConfFileExists() {
    assertThrows(RedisConnectionException.class, () -> readFromResources("wrong.file.name"));
  }

  @Test
  void testReadFromResources() {
    var expectedRedisProperties = new RedisConf("localhost", 6379);
    var actualRedisProperties = readFromResources("redis.properties");
    Assertions.assertEquals(expectedRedisProperties, actualRedisProperties);
  }
}

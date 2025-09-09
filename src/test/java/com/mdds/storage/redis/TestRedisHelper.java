/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

import static com.mdds.storage.redis.RedisHelper.readFromFile;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestRedisHelper {

  @Test
  void testNoConfFileExists() {
    assertThrows(RedisConnectionException.class, () -> readFromFile("wrong.file.name"));
  }

  @Test
  void testReadFromFile() {
    var expectedRedisProperties = new RedisProperties("localhost", 6379);
    var actualRedisProperties = readFromFile("redis.properties");
    Assertions.assertEquals(expectedRedisProperties, actualRedisProperties);
  }
}

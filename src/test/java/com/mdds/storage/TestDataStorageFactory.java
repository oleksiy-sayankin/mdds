/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage;

import static com.mdds.storage.redis.RedisConf.DEFAULT_HOST;
import static com.mdds.util.CustomHelper.findFreePort;

import com.mdds.storage.redis.RedisConf;
import dto.ResultDTO;
import dto.TaskStatus;
import java.io.IOException;
import java.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.embedded.RedisServer;

class TestDataStorageFactory {
  private static final int REDIS_SERVER_PORT = findFreePort();
  private static RedisServer redisServer;

  @BeforeAll
  static void startServer() throws IOException {
    redisServer = new RedisServer(REDIS_SERVER_PORT);
    redisServer.start();
  }

  @AfterAll
  static void stopServer() throws IOException {
    if (redisServer != null) {
      redisServer.stop();
    }
  }

  @Test
  void testPut() {
    var result = new ResultDTO();
    var taskId = "test";
    result.setTaskId(taskId);
    result.setDateTimeTaskCreated(Instant.now());
    result.setDateTimeTaskFinished(Instant.now());
    result.setTaskStatus(TaskStatus.DONE);
    result.setSolution(new double[] {1.1, 2.2, 3.3, 4.4});
    result.setErrorMessage("");
    try (var dataStorage =
        DataStorageFactory.createRedis(new RedisConf(DEFAULT_HOST, REDIS_SERVER_PORT))) {
      Assertions.assertDoesNotThrow(() -> dataStorage.put(taskId, result));
    }
  }

  @Test
  void testGet() {
    var expectedResult = new ResultDTO();
    var taskId = "test";
    expectedResult.setTaskId(taskId);
    expectedResult.setDateTimeTaskCreated(Instant.now());
    expectedResult.setDateTimeTaskFinished(Instant.now());
    expectedResult.setTaskStatus(TaskStatus.DONE);
    expectedResult.setSolution(new double[] {1.1, 2.2, 3.3, 4.4});
    expectedResult.setErrorMessage("");
    try (var dataStorage =
        DataStorageFactory.createRedis(new RedisConf(DEFAULT_HOST, REDIS_SERVER_PORT))) {
      Assertions.assertDoesNotThrow(() -> dataStorage.put(taskId, expectedResult));
      var actualResult = dataStorage.get(taskId, ResultDTO.class);
      Assertions.assertEquals(
          expectedResult, actualResult.isPresent() ? actualResult.get() : actualResult);
    }
  }

  @Test
  void testGetAndRedisWithParams() {
    var expectedResult = new ResultDTO();
    var taskId = "test";
    expectedResult.setTaskId(taskId);
    expectedResult.setDateTimeTaskCreated(Instant.now());
    expectedResult.setDateTimeTaskFinished(Instant.now());
    expectedResult.setTaskStatus(TaskStatus.DONE);
    expectedResult.setSolution(new double[] {1.1, 2.2, 3.3, 4.4});
    expectedResult.setErrorMessage("");
    try (var dataStorage = DataStorageFactory.createRedis("localhost", REDIS_SERVER_PORT)) {
      Assertions.assertDoesNotThrow(() -> dataStorage.put(taskId, expectedResult));
      var actualResult = dataStorage.get(taskId, ResultDTO.class);
      Assertions.assertEquals(
          expectedResult, actualResult.isPresent() ? actualResult.get() : actualResult);
    }
  }

  @Test
  void testClose() {
    try (var dataStorage =
        DataStorageFactory.createRedis(new RedisConf(DEFAULT_HOST, REDIS_SERVER_PORT))) {
      Assertions.assertDoesNotThrow(
          () -> {
            // Do nothing
          });
    }
  }

  @Test
  void testGetNull() {
    try (var dataStorage =
        DataStorageFactory.createRedis(new RedisConf(DEFAULT_HOST, REDIS_SERVER_PORT))) {
      Assertions.assertDoesNotThrow(
          () -> Assertions.assertFalse(dataStorage.get("random key", ResultDTO.class).isPresent()));
    }
  }
}

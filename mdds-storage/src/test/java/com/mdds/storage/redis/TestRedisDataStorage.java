/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

import static com.mdds.common.util.CommonHelper.findFreePort;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.mdds.dto.ResultDTO;
import com.mdds.dto.TaskStatus;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.embedded.RedisServer;

class TestRedisDataStorage {
  private static final String DEFAULT_HOST = "localhost";
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
    result.setPercentDone(100);
    result.setSolution(new double[] {9.3, 6.278, 6.783, 3.874});
    result.setErrorMessage("");
    try (var dataStorage = new RedisDataStorage(new RedisConf(DEFAULT_HOST, REDIS_SERVER_PORT))) {
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
    expectedResult.setPercentDone(100);
    expectedResult.setSolution(new double[] {81.1, 82.2, 37.3, 45.497});
    expectedResult.setErrorMessage("");
    try (var dataStorage = new RedisDataStorage(DEFAULT_HOST, REDIS_SERVER_PORT)) {
      Assertions.assertDoesNotThrow(() -> dataStorage.put(taskId, expectedResult));
      var actualResult = dataStorage.get(taskId, ResultDTO.class);
      Assertions.assertEquals(
          expectedResult, actualResult.isPresent() ? actualResult.get() : actualResult);
    }
  }

  @Test
  void testClose() {
    try (var ignore = new RedisDataStorage(DEFAULT_HOST, REDIS_SERVER_PORT)) {
      Assertions.assertDoesNotThrow(
          () -> {
            // Do nothing
          });
    }
  }

  @Test
  void testNoConfFileExists() {
    var randomHost = "random.host";
    var randomPort = 89798;
    var oneSecond = Duration.ofSeconds(1);
    assertThrows(
        RedisConnectionException.class,
        () -> {
          try (var ignore = new RedisDataStorage(randomHost, randomPort, oneSecond)) {
            // Do nothing.
          }
        });
  }

  @Test
  void testWrongConfFile() {
    var properties = RedisHelper.readFromResources("no.connection.redis.properties");
    var oneSecond = Duration.ofSeconds(1);
    assertThrows(
        RedisConnectionException.class,
        () -> {
          try (var ignore = new RedisDataStorage(properties, oneSecond)) {
            // Do nothing.
          }
        });
  }
}

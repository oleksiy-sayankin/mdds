/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

import static com.mdds.storage.redis.RedisProperties.DEFAULT_HOST;
import static com.mdds.storage.redis.RedisProperties.DEFAULT_PORT;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dto.ResultDTO;
import dto.TaskStatus;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** We expect that Redis service is up and running for these tests. */
class TestRedisDataStorage {
  @Test
  void testPut() {
    var result = new ResultDTO();
    var taskId = "test";
    result.setTaskId(taskId);
    result.setDateTimeTaskCreated(Instant.now());
    result.setDateTimeTaskFinished(Instant.now());
    result.setTaskStatus(TaskStatus.DONE);
    result.setSolution(new double[] {9.3, 6.278, 6.783, 3.874});
    result.setErrorMessage("");
    try (var dataStorage = new RedisDataStorage(new RedisProperties(DEFAULT_HOST, DEFAULT_PORT))) {
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
    expectedResult.setSolution(new double[] {81.1, 82.2, 37.3, 45.497});
    expectedResult.setErrorMessage("");
    try (var dataStorage = new RedisDataStorage(DEFAULT_HOST, DEFAULT_PORT)) {
      Assertions.assertDoesNotThrow(() -> dataStorage.put(taskId, expectedResult));
      var actualResult = dataStorage.get(taskId, ResultDTO.class);
      Assertions.assertEquals(
          expectedResult, actualResult.isPresent() ? actualResult.get() : actualResult);
    }
  }

  @Test
  void testClose() {
    try (var dataStorage = new RedisDataStorage(DEFAULT_HOST, DEFAULT_PORT)) {
      Assertions.assertDoesNotThrow(
          () -> {
            // Do nothing
          });
    }
  }

  @Test
  void testNoConfFileExists() {
    String randomHost = "random.host";
    int randomPort = 89798;
    assertThrows(
        RedisConnectionException.class,
        () -> {
          try (var dataStorage = new RedisDataStorage(randomHost, randomPort)) {
            // Do nothing.
          }
        });
  }

  @Test
  void testWrongConfFile() {
    var properties = RedisHelper.readFromResources("no.connection.redis.properties");
    assertThrows(
        RedisConnectionException.class,
        () -> {
          try (var dataStorage = new RedisDataStorage(properties)) {
            // Do nothing.
          }
        });
  }
}

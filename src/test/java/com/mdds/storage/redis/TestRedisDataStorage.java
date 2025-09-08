/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

import static org.junit.jupiter.api.Assertions.assertNull;

import com.mdds.storage.DataStorageFactory;
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
    result.setSolution(new double[] {1.1, 2.2, 3.3, 4.4});
    result.setErrorMessage("");
    try (var dataStorage = DataStorageFactory.createRedis()) {
      Assertions.assertDoesNotThrow(
          () -> {
            dataStorage.put(taskId, result);
          });
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
    try (var dataStorage = DataStorageFactory.createRedis()) {
      Assertions.assertDoesNotThrow(
          () -> {
            dataStorage.put(taskId, expectedResult);
          });
      var actualResult = dataStorage.get(taskId, ResultDTO.class);
      Assertions.assertEquals(expectedResult, actualResult);
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
    try (var dataStorage = DataStorageFactory.createRedis("localhost", 6379)) {
      Assertions.assertDoesNotThrow(
          () -> dataStorage.put(taskId, expectedResult));
      var actualResult = dataStorage.get(taskId, ResultDTO.class);
      Assertions.assertEquals(expectedResult, actualResult);
    }
  }

  @Test
  void testClose() {
    try (var dataStorage = DataStorageFactory.createRedis()) {
      Assertions.assertDoesNotThrow(
          () -> {
            // Do nothing
          });
    }
  }

  @Test
  void testGetNull() {
    try (var dataStorage = DataStorageFactory.createRedis()) {
      Assertions.assertDoesNotThrow(
          () -> {
            assertNull(dataStorage.get("random key", ResultDTO.class));
          });
    }
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

import static org.junit.jupiter.api.Assertions.assertNull;

import dto.ResultDTO;
import dto.TaskStatus;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestRedisDataStorage {

  @Test
  void testConnection() {
    var dataStorage = RedisDataStorage.getInstance();
    Assertions.assertDoesNotThrow(dataStorage::connect);
  }

  @Test
  void testPut() {
    var dataStorage = RedisDataStorage.getInstance();
    dataStorage.connect();
    var result = new ResultDTO();
    var taskId = "test";
    result.setTaskId(taskId);
    result.setDateTimeTaskCreated(Instant.now());
    result.setDateTimeTaskFinished(Instant.now());
    result.setTaskStatus(TaskStatus.DONE);
    result.setSolution(new double[] {1.1, 2.2, 3.3, 4.4});
    result.setErrorMessage("");
    Assertions.assertDoesNotThrow(
        () -> {
          dataStorage.put(taskId, result);
        });
    dataStorage.close();
  }

  @Test
  void testGet() {
    var dataStorage = RedisDataStorage.getInstance();
    dataStorage.connect();
    var expectedResult = new ResultDTO();
    var taskId = "test";
    expectedResult.setTaskId(taskId);
    expectedResult.setDateTimeTaskCreated(Instant.now());
    expectedResult.setDateTimeTaskFinished(Instant.now());
    expectedResult.setTaskStatus(TaskStatus.DONE);
    expectedResult.setSolution(new double[] {1.1, 2.2, 3.3, 4.4});
    expectedResult.setErrorMessage("");
    Assertions.assertDoesNotThrow(
        () -> {
          dataStorage.put(taskId, expectedResult);
        });
    var actualResult = dataStorage.get(taskId, ResultDTO.class);
    Assertions.assertEquals(expectedResult, actualResult);
    dataStorage.close();
  }

  @Test
  void testClose() {
    var dataStorage = RedisDataStorage.getInstance();
    dataStorage.connect();
    Assertions.assertDoesNotThrow(dataStorage::close);
  }

  @Test
  void testGetNull() {
    var dataStorage = RedisDataStorage.getInstance();
    dataStorage.connect();
    assertNull(dataStorage.get("random key", ResultDTO.class));
  }
}

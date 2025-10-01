/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.util;

import static dto.SlaeSolver.NUMPY_EXACT_SOLVER;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dto.TaskDTO;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestJsonHelper {

  @Test
  void testToJson() {
    var task = new TaskDTO();
    var taskId = "testId";
    var isoFormattedString = "2024-09-04T16:21:00Z";
    var instant = Instant.parse(isoFormattedString);
    task.setId(taskId);
    task.setDateTime(instant);
    task.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    task.setMatrix(new double[][] {{2.2, 3.3}, {4.5, 6.3}});
    task.setRhs(new double[] {4.7, 8.9});
    var actualJson = JsonHelper.toJson(task);
    var expectedJson =
        "{\"id\":\"testId\",\"dateTime\":1725466860.000000000,\"matrix\":[[2.2,3.3],[4.5,6.3]],\"rhs\":[4.7,8.9],\"slaeSolvingMethod\":\"NUMPY_EXACT_SOLVER\"}";
    Assertions.assertEquals(expectedJson, actualJson);
  }

  @Test
  void testFromJson() {
    var json =
        "{\"id\":\"testId\",\"dateTime\":1725466860.000000000,\"matrix\":[[2.2,3.3],[4.5,6.3]],\"rhs\":[4.7,8.9],\"slaeSolvingMethod\":\"NUMPY_EXACT_SOLVER\"}";
    var expectedTask = new TaskDTO();
    var taskId = "testId";
    var isoFormattedString = "2024-09-04T16:21:00Z";
    var instant = Instant.parse(isoFormattedString);
    expectedTask.setId(taskId);
    expectedTask.setDateTime(instant);
    expectedTask.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    expectedTask.setMatrix(new double[][] {{2.2, 3.3}, {4.5, 6.3}});
    expectedTask.setRhs(new double[] {4.7, 8.9});
    var actualTask = JsonHelper.fromJson(json, TaskDTO.class);
    Assertions.assertEquals(expectedTask, actualTask);
  }

  @Test
  void testFromJsonNullCheck() {
    var actualTask = JsonHelper.fromJson(null, TaskDTO.class);
    Assertions.assertNull(actualTask);
  }

  @Test
  void testFromJsonWithException() {
    var corruptedJson = "{rfe}fewr";
    assertThrows(
        JsonException.class,
        () -> {
          JsonHelper.fromJson(corruptedJson, TaskDTO.class);
        });
  }

  @Test
  void testToJsonWithException() {
    var test = new NotConvertableToJson() {};
    assertThrows(
        JsonException.class,
        () -> {
          JsonHelper.toJson(test);
        });
  }

  private abstract static class NotConvertableToJson {}
}

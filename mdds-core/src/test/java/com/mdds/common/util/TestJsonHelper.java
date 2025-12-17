/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common.util;

import static com.mdds.dto.SlaeSolver.NUMPY_EXACT_SOLVER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.mdds.dto.ResultDTO;
import com.mdds.dto.TaskDTO;
import com.mdds.dto.TaskIdResponseDTO;
import com.mdds.dto.TaskStatus;
import java.time.Instant;
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
    assertThat(actualJson).isEqualTo(expectedJson);
  }

  @Test
  void testToJsonResultDto() {
    var taskId = "testId";
    var isoFormattedString = "2024-09-04T16:21:00Z";
    var instant = Instant.parse(isoFormattedString);
    var resultDTO =
        new ResultDTO(
            taskId, instant, instant, TaskStatus.DONE, 100, new double[] {1.971, 3.213, 7.243}, "");
    var actualJson = JsonHelper.toJson(resultDTO);
    var expectedJson =
        "{"
            + "\"taskId\":\"testId\","
            + "\"dateTimeTaskCreated\":1725466860.000000000,"
            + "\"dateTimeTaskFinished\":1725466860.000000000,"
            + "\"taskStatus\":\"DONE\","
            + "\"progress\":100,"
            + "\"solution\":[1.971,3.213,7.243],"
            + "\"errorMessage\":\"\""
            + "}";
    assertThat(actualJson).isEqualTo(expectedJson);
  }

  @Test
  void testFromJsonResultDto() {
    var json =
        "{"
            + "\"taskId\":\"testId\","
            + "\"dateTimeTaskCreated\":1725466860.000000000,"
            + "\"dateTimeTaskFinished\":1725466860.000000000,"
            + "\"taskStatus\":\"DONE\","
            + "\"progress\":100,"
            + "\"solution\":[1.971,3.213,7.243],"
            + "\"errorMessage\":\"\""
            + "}";
    var actualResult = JsonHelper.fromJson(json, ResultDTO.class);
    var taskId = "testId";
    var isoFormattedString = "2024-09-04T16:21:00Z";
    var instant = Instant.parse(isoFormattedString);
    var expectedResult =
        new ResultDTO(
            taskId, instant, instant, TaskStatus.DONE, 100, new double[] {1.971, 3.213, 7.243}, "");
    assertThat(actualResult).isEqualTo(expectedResult);
  }

  @Test
  void testToJsonTaskIdResponseDTO() {
    var taskIdResponse = new TaskIdResponseDTO("87a027b0-beb7-4171-8fbf-7b7568dce461");
    var actualJson = JsonHelper.toJson(taskIdResponse);
    var expectedJson = "{\"id\":\"87a027b0-beb7-4171-8fbf-7b7568dce461\"}";
    assertThat(actualJson).isEqualTo(expectedJson);
  }

  @Test
  void testFromJsonTaskIdResponseDTO() {
    var json = "{\"id\":\"87a027b0-beb7-4171-8fbf-7b7568dce461\"}";
    var expectedTaskIdResponseDTO = new TaskIdResponseDTO("87a027b0-beb7-4171-8fbf-7b7568dce461");
    var actualTaskIdResponseDTO = JsonHelper.fromJson(json, TaskIdResponseDTO.class);
    assertThat(actualTaskIdResponseDTO).isEqualTo(expectedTaskIdResponseDTO);
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
    assertThat(actualTask).isEqualTo(expectedTask);
  }

  @Test
  void testFromJsonNullCheck() {
    var actualTask = JsonHelper.fromJson(null, TaskDTO.class);
    assertThat(actualTask).isNull();
  }

  @Test
  void testFromJsonWithException() {
    var corruptedJson = "{rfe}fewr";
    assertThatThrownBy(() -> JsonHelper.fromJson(corruptedJson, TaskDTO.class))
        .isInstanceOf(JsonException.class)
        .hasMessageContaining("Could not convert JSON to object");
  }

  @Test
  void testToJsonWithException() {
    var test = new NotConvertableToJson() {};
    assertThatThrownBy(() -> JsonHelper.toJson(test))
        .isInstanceOf(JsonException.class)
        .hasMessageContaining("Could not convert object to JSON");
  }

  private abstract static class NotConvertableToJson {}
}

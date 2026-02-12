/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common.util;

import static com.mdds.dto.SlaeSolver.NUMPY_EXACT_SOLVER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.mdds.dto.JobDTO;
import com.mdds.dto.JobIdResponseDTO;
import com.mdds.dto.ResultDTO;
import com.mdds.grpc.solver.JobStatus;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class TestJsonHelper {

  @Test
  void testToJson() {
    var job = new JobDTO();
    var jobId = "testId";
    var isoFormattedString = "2024-09-04T16:21:00Z";
    var instant = Instant.parse(isoFormattedString);
    job.setId(jobId);
    job.setDateTime(instant);
    job.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    job.setMatrix(new double[][] {{2.2, 3.3}, {4.5, 6.3}});
    job.setRhs(new double[] {4.7, 8.9});
    var actualJson = JsonHelper.toJson(job);
    var expectedJson =
        "{\"id\":\"testId\",\"dateTime\":1725466860.000000000,\"matrix\":[[2.2,3.3],[4.5,6.3]],\"rhs\":[4.7,8.9],\"slaeSolvingMethod\":\"NUMPY_EXACT_SOLVER\"}";
    assertThat(actualJson).isEqualTo(expectedJson);
  }

  @Test
  void testToJsonResultDto() {
    var jobId = "testId";
    var isoFormattedString = "2024-09-04T16:21:00Z";
    var instant = Instant.parse(isoFormattedString);
    var resultDTO =
        new ResultDTO(
            jobId,
            instant,
            instant,
            JobStatus.DONE,
            "cancel.queue-executor-0001",
            100,
            new double[] {1.971, 3.213, 7.243},
            "");
    var actualJson = JsonHelper.toJson(resultDTO);
    var expectedJson =
        "{"
            + "\"jobId\":\"testId\","
            + "\"dateTimeJobStarted\":1725466860.000000000,"
            + "\"dateTimeJobEnded\":1725466860.000000000,"
            + "\"jobStatus\":\"DONE\","
            + "\"cancelQueueName\":\"cancel.queue-executor-0001\","
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
            + "\"jobId\":\"testId\","
            + "\"dateTimeJobStarted\":1725466860.000000000,"
            + "\"dateTimeJobEnded\":1725466860.000000000,"
            + "\"jobStatus\":\"DONE\","
            + "\"cancelQueueName\":\"cancel.queue-executor-0001\","
            + "\"progress\":100,"
            + "\"solution\":[1.971,3.213,7.243],"
            + "\"errorMessage\":\"\""
            + "}";
    var actualResult = JsonHelper.fromJson(json, ResultDTO.class);
    var jobId = "testId";
    var isoFormattedString = "2024-09-04T16:21:00Z";
    var instant = Instant.parse(isoFormattedString);
    var expectedResult =
        new ResultDTO(
            jobId,
            instant,
            instant,
            JobStatus.DONE,
            "cancel.queue-executor-0001",
            100,
            new double[] {1.971, 3.213, 7.243},
            "");
    assertThat(actualResult).isEqualTo(expectedResult);
  }

  @Test
  void testToJsonJobIdResponseDTO() {
    var jobIdResponse = new JobIdResponseDTO("87a027b0-beb7-4171-8fbf-7b7568dce461");
    var actualJson = JsonHelper.toJson(jobIdResponse);
    var expectedJson = "{\"id\":\"87a027b0-beb7-4171-8fbf-7b7568dce461\"}";
    assertThat(actualJson).isEqualTo(expectedJson);
  }

  @Test
  void testFromJsonJobIdResponseDTO() {
    var json = "{\"id\":\"87a027b0-beb7-4171-8fbf-7b7568dce461\"}";
    var expectedJobIdResponseDTO = new JobIdResponseDTO("87a027b0-beb7-4171-8fbf-7b7568dce461");
    var actualJobIdResponseDTO = JsonHelper.fromJson(json, JobIdResponseDTO.class);
    assertThat(actualJobIdResponseDTO).isEqualTo(expectedJobIdResponseDTO);
  }

  @Test
  void testFromJson() {
    var json =
        "{\"id\":\"testId\",\"dateTime\":1725466860.000000000,\"matrix\":[[2.2,3.3],[4.5,6.3]],\"rhs\":[4.7,8.9],\"slaeSolvingMethod\":\"NUMPY_EXACT_SOLVER\"}";
    var expectedJob = new JobDTO();
    var jobId = "testId";
    var isoFormattedString = "2024-09-04T16:21:00Z";
    var instant = Instant.parse(isoFormattedString);
    expectedJob.setId(jobId);
    expectedJob.setDateTime(instant);
    expectedJob.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    expectedJob.setMatrix(new double[][] {{2.2, 3.3}, {4.5, 6.3}});
    expectedJob.setRhs(new double[] {4.7, 8.9});
    var actualJob = JsonHelper.fromJson(json, JobDTO.class);
    assertThat(actualJob).isEqualTo(expectedJob);
  }

  @Test
  void testFromJsonNullCheck() {
    var actualJob = JsonHelper.fromJson(null, JobDTO.class);
    assertThat(actualJob).isNull();
  }

  @Test
  void testFromJsonWithException() {
    var corruptedJson = "{rfe}fewr";
    assertThatThrownBy(() -> JsonHelper.fromJson(corruptedJson, JobDTO.class))
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

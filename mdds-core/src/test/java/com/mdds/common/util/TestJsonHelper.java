/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.mdds.dto.rest.v1.CreateJobResponseDTO;
import org.junit.jupiter.api.Test;

class TestJsonHelper {

  @Test
  void testToJsonJobIdResponseDTO() {
    var jobIdResponse = new CreateJobResponseDTO("87a027b0-beb7-4171-8fbf-7b7568dce461");
    var actualJson = JsonHelper.toJson(jobIdResponse);
    var expectedJson = "{\"jobId\":\"87a027b0-beb7-4171-8fbf-7b7568dce461\"}";
    assertThat(actualJson).isEqualTo(expectedJson);
  }

  @Test
  void testFromJsonJobIdResponseDTO() {
    var json = "{\"jobId\":\"87a027b0-beb7-4171-8fbf-7b7568dce461\"}";
    var expectedJobIdResponseDTO = new CreateJobResponseDTO("87a027b0-beb7-4171-8fbf-7b7568dce461");
    var actualJobIdResponseDTO = JsonHelper.fromJson(json, CreateJobResponseDTO.class);
    assertThat(actualJobIdResponseDTO).isEqualTo(expectedJobIdResponseDTO);
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

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.test;

import com.mdds.common.util.JsonHelper;
import com.mdds.dto.SlaeSolver;
import com.mdds.dto.TaskIdResponseDTO;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestHttpRequestDataSource extends BaseEnvironment {

  @ParameterizedTest
  @MethodSource("solvers")
  void testHttpRequest(SlaeSolver solver) throws IOException, InterruptedException {
    Map<String, Object> params = new HashMap<>();
    params.put(
        "matrix",
        List.of(List.of(1.3, 2.4, 3.1), List.of(4.77, 5.2321, 6.32), List.of(7.23, 8.43, 9.4343)));
    params.put("rhs", List.of(1.3, 2.2, 3.7));
    var response = webServerClient.postSolve("http_request", solver.getName(), params);
    var json = response.body();

    var taskId = JsonHelper.fromJson(json, TaskIdResponseDTO.class).getId();
    Assertions.assertThat(taskId).as("Task id should not be null").isNotNull();
    var actual = awaitForResult(taskId);

    double[] expected = {
      -0.3291566787737896398658, 0.7293212011512698153684, -0.0072474839861680725996
    };

    assertDoneAndEquals(expected, actual);
  }
}

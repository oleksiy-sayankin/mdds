/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.test;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import com.mdds.common.util.JsonHelper;
import com.mdds.dto.SlaeSolver;
import com.mdds.dto.TaskIdResponseDTO;
import io.restassured.http.ContentType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestHttpRequestDataSource extends BaseEnvironment {

  @ParameterizedTest
  @MethodSource("solvers")
  void testHttpRequest(SlaeSolver solver) {
    var json =
        given()
            .multiPart("slaeSolvingMethod", solver.getName())
            .multiPart("dataSourceType", "http_request")
            .multiPart(
                "matrix",
                "matrix.csv",
                "1.3,2.4,3.1\n4.77,5.2321,6.32\n7.23,8.43,9.4343\n".getBytes())
            .multiPart("rhs", "rhs.csv", "1.3\n2.2\n3.7\n".getBytes())
            .when()
            .post("/solve")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .extract()
            .asString();

    var taskId = JsonHelper.fromJson(json, TaskIdResponseDTO.class).getId();
    assertThat(taskId).as("Task id should not be null").isNotNull();
    var actual = awaitForResult(taskId);

    double[] expected = {
      -0.3291566787737896398658, 0.7293212011512698153684, -0.0072474839861680725996
    };

    assertDoneAndEquals(expected, actual);
  }
}

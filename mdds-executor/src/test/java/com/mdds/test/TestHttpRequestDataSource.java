/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.mdds.common.util.JsonHelper;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.SlaeSolver;
import com.mdds.dto.TaskIdResponseDTO;
import com.mdds.dto.TaskStatus;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestHttpRequestDataSource extends BaseEnvironment {

  @ParameterizedTest
  @MethodSource("solvers")
  void testHttpRequest(SlaeSolver slaeSolver) throws Exception {
    var uri =
        new AtomicReference<>(
            new URI(
                "http://"
                    + WEB_SERVER.getHost()
                    + ":"
                    + WEB_SERVER.getMappedPort(MDDS_WEB_SERVER_PORT)
                    + "/solve"));
    var url = new AtomicReference<>(uri.get().toURL());

    var boundary = "----TestBoundary";
    var connection = new AtomicReference<>((HttpURLConnection) url.get().openConnection());
    connection.get().setDoOutput(true);
    connection.get().setRequestMethod("POST");
    connection
        .get()
        .setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

    try (var output = connection.get().getOutputStream()) {
      var writer = new PrintWriter(new OutputStreamWriter(output, UTF_8), true);

      // add slaeSolvingMethod
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"slaeSolvingMethod\"\r\n\r\n");
      writer.append(slaeSolver.getName()).append("\r\n");
      writer.flush();

      // add dataSourceType
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"dataSourceType\"\r\n\r\n");
      writer.append("http_request").append("\r\n");
      writer.flush();

      // add matrix
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"matrix\"; filename=\"matrix.csv\"\r\n");
      writer.append("Content-Type: text/csv\r\n\r\n");
      writer.flush();
      output.write("1.3,2.4,3.1\n4.77,5.2321,6.32\n7.23,8.43,9.4343\n".getBytes(UTF_8));
      output.write("\r\n".getBytes(UTF_8));
      output.flush();

      // put rhs
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"rhs\"; filename=\"rhs.csv\"\r\n");
      writer.append("Content-Type: text/csv\r\n\r\n");
      writer.flush();
      output.write("1.3\n2.2\n3.7\n".getBytes(UTF_8));
      output.write("\r\n".getBytes(UTF_8));
      output.flush();

      // finish the request
      writer.append("--").append(boundary).append("--").append("\r\n");
      writer.close();
    }

    // Check the answer
    assertEquals(HttpURLConnection.HTTP_OK, connection.get().getResponseCode());
    assertEquals("application/json;charset=ISO-8859-1", connection.get().getContentType());

    TaskIdResponseDTO response;
    try (var reader =
        new BufferedReader(new InputStreamReader(connection.get().getInputStream()))) {
      var body = reader.lines().reduce("", (a, b) -> a + b);
      assertTrue(body.contains("id"));
      response = JsonHelper.fromJson(body, TaskIdResponseDTO.class);
    }
    var id = response.getId();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              uri.set(
                  new URI(
                      "http://"
                          + WEB_SERVER.getHost()
                          + ":"
                          + WEB_SERVER.getMappedPort(MDDS_WEB_SERVER_PORT)
                          + "/result/"
                          + id));
              url.set(uri.get().toURL());
              connection.set((HttpURLConnection) url.get().openConnection());
              connection.get().setRequestMethod("GET");

              assertEquals(HttpURLConnection.HTTP_OK, connection.get().getResponseCode());
              assertEquals(
                  "application/json;charset=ISO-8859-1", connection.get().getContentType());

              ResultDTO actualResult;
              try (var reader =
                  new BufferedReader(new InputStreamReader(connection.get().getInputStream()))) {
                var body = reader.lines().reduce("", (a, b) -> a + b);
                actualResult = JsonHelper.fromJson(body, ResultDTO.class);
              }
              assertSame(TaskStatus.DONE, actualResult.getTaskStatus());

              var expectedResult =
                  new double[] {
                    -0.3291566787737896398658, 0.7293212011512698153684, -0.0072474839861680725996
                  };
              var delta = 0.00000001;
              assertArrayEquals(expectedResult, actualResult.getSolution(), delta);
            });
  }
}

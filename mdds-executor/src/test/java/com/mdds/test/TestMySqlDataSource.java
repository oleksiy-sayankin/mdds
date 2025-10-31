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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class TestMySqlDataSource extends BaseEnvironment {
  private static final String DB_NAME = "testdb";
  private static final String USER_NAME = "testuser";
  private static final String PASSWORD = "testpass";
  private static final String MYSQL_HOST = "mysqlhost";
  private static final String MYSQL_URL =
      "jdbc:mysql://" + MYSQL_HOST + ":" + MySQLContainer.MYSQL_PORT + "/" + DB_NAME;

  @SuppressWarnings("resource")
  @Container
  private static final MySQLContainer<?> MY_SQL_CONTAINER =
      new MySQLContainer<>("mysql:8.4.6")
          .withDatabaseName(DB_NAME)
          .withUsername(USER_NAME)
          .withPassword(PASSWORD)
          .withNetwork(SHARED_NETWORK)
          .withNetworkAliases(MYSQL_HOST);

  @BeforeAll
  static void setupMySql() throws SQLException {
    awaitReady(TestMySqlDataSource::mysqlIsReady, "MySql");
    createMySqlTestData();
  }

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
      writer.append("mysql").append("\r\n");
      writer.flush();

      // add mysqlUrl
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlUrl\"\r\n\r\n");
      writer.append(MYSQL_URL).append("\r\n");
      writer.flush();

      // add mysqlUser
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlUser\"\r\n\r\n");
      writer.append(USER_NAME).append("\r\n");
      writer.flush();

      // add mysqlPassword
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlPassword\"\r\n\r\n");
      writer.append(PASSWORD).append("\r\n");
      writer.flush();

      // add mysqlDbName
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlDbName\"\r\n\r\n");
      writer.append(DB_NAME).append("\r\n");
      writer.flush();

      // add mysqlMatrixTableName
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlMatrixTableName\"\r\n\r\n");
      writer.append("MATRIX_TABLE").append("\r\n");
      writer.flush();

      // add mysqlMatrixJsonFieldName
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlMatrixJsonFieldName\"\r\n\r\n");
      writer.append("JSON_FIELD").append("\r\n");
      writer.flush();

      // add mysqlMatrixPrimaryKeyFieldName
      writer.append("--").append(boundary).append("\r\n");
      writer.append(
          "Content-Disposition: form-data; name=\"mysqlMatrixPrimaryKeyFieldName\"\r\n\r\n");
      writer.append("ID").append("\r\n");
      writer.flush();

      // add mysqlMatrixPrimaryKeyFieldValue
      writer.append("--").append(boundary).append("\r\n");
      writer.append(
          "Content-Disposition: form-data; name=\"mysqlMatrixPrimaryKeyFieldValue\"\r\n\r\n");
      writer.append("1").append("\r\n");
      writer.flush();

      // add mysqlRhsTableName
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlRhsTableName\"\r\n\r\n");
      writer.append("RHS_TABLE").append("\r\n");
      writer.flush();

      // add mysqlRhsJsonFieldName
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlRhsJsonFieldName\"\r\n\r\n");
      writer.append("JSON_FIELD").append("\r\n");
      writer.flush();

      // add mysqlRhsPrimaryKeyFieldName
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlRhsPrimaryKeyFieldName\"\r\n\r\n");
      writer.append("ID").append("\r\n");
      writer.flush();

      // add mysqlRhsPrimaryKeyFieldValue
      writer.append("--").append(boundary).append("\r\n");
      writer.append(
          "Content-Disposition: form-data; name=\"mysqlRhsPrimaryKeyFieldValue\"\r\n\r\n");
      writer.append("1").append("\r\n");
      writer.flush();

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
                    3.8673716012084592145015, -1.8960725075528700906344, 0.8996978851963746223565
                  };
              var delta = 0.00000001;
              assertArrayEquals(expectedResult, actualResult.getSolution(), delta);
            });
  }

  private static void createMySqlTestData() throws SQLException {
    var jdbcUrl = MY_SQL_CONTAINER.getJdbcUrl();
    try (var connection = DriverManager.getConnection(jdbcUrl, USER_NAME, PASSWORD);
        var stmt = connection.createStatement()) {
      stmt.execute("USE " + DB_NAME);
      stmt.execute("CREATE TABLE MATRIX_TABLE (ID INT PRIMARY KEY, JSON_FIELD VARCHAR(255))");
      stmt.execute("CREATE TABLE RHS_TABLE (ID INT PRIMARY KEY, JSON_FIELD VARCHAR(255))");
      stmt.execute(
          "INSERT INTO MATRIX_TABLE (ID, JSON_FIELD) VALUES (1,"
              + " '[[3.4,5.5,2.2],[1.2,5.5,8.1],[3.4,8.6,9.4]]')");
      stmt.execute("INSERT INTO RHS_TABLE (ID, JSON_FIELD) VALUES (1,'[4.7,1.5,5.3]')");
    }
  }

  private static boolean mysqlIsReady() {
    try (var conn =
        DriverManager.getConnection(MY_SQL_CONTAINER.getJdbcUrl(), USER_NAME, PASSWORD)) {
      return conn.isValid(2);
    } catch (SQLException e) {
      return false;
    }
  }
}

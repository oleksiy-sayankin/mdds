/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.test;

import com.mdds.common.util.JsonHelper;
import com.mdds.dto.SlaeSolver;
import com.mdds.dto.TaskIdResponseDTO;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.assertj.core.api.Assertions;
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
  void testHttpRequest(SlaeSolver solver) throws IOException, InterruptedException {
    Map<String, Object> params = new HashMap<>();
    params.put("mysql.url", MYSQL_URL);
    params.put("mysql.user", USER_NAME);
    params.put("mysql.password", PASSWORD);
    params.put("mysql.db.name", DB_NAME);
    params.put("mysql.matrix.table.name", "MATRIX_TABLE");
    params.put("mysql.matrix.json.field.name", "JSON_FIELD");
    params.put("mysql.matrix.primary.key.field.name", "ID");
    params.put("mysql.matrix.primary.key.field.value", "1");
    params.put("mysql.rhs.table.name", "RHS_TABLE");
    params.put("mysql.rhs.json.field.name", "JSON_FIELD");
    params.put("mysql.rhs.primary.key.field.name", "ID");
    params.put("mysql.rhs.primary.key.field.value", "1");
    var response = webServerClient.postSolve("mysql", solver.getName(), params);
    var json = response.body();

    var taskId = JsonHelper.fromJson(json, TaskIdResponseDTO.class).getId();
    Assertions.assertThat(taskId).as("Task id should not be null").isNotNull();
    var actual = awaitForResult(taskId);

    double[] expected = {
      3.8673716012084592145015, -1.8960725075528700906344, 0.8996978851963746223565
    };

    assertDoneAndEquals(expected, actual);
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

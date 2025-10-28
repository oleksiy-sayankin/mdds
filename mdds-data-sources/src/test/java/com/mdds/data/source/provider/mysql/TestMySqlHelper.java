/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.mysql;

import static com.mdds.data.source.provider.mysql.MySqlHelper.*;

import com.mdds.common.util.JsonHelper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TestMySqlHelper {
  private static Connection connection;

  @BeforeAll
  static void init() throws SQLException {
    connection = getH2Connection();
    initializeDatabase(connection);
  }

  @AfterAll
  static void close() throws SQLException {
    connection.close();
  }

  @Test
  void testBuildMatrixQuery() {
    Map<String, Object> params = new HashMap<>();
    params.put("mysql.db.name", "db_name");
    params.put("mysql.matrix.table.name", "matrix_table");
    params.put("mysql.matrix.json.field.name", "json_filed");
    params.put("mysql.matrix.primary.key.field.name", "id");
    params.put("mysql.matrix.primary.key.field.value", "1984");
    var mySqlConfig = MySqlConfig.of(params);
    var actualQuery = buildMatrixQuery(mySqlConfig);
    var expectedQuery = "SELECT json_filed FROM db_name.matrix_table WHERE id = ?";
    Assertions.assertEquals(expectedQuery, actualQuery);
  }

  @Test
  void testNoConnection() {
    Map<String, Object> params = new HashMap<>();
    params.put("mysql.db.name", "db_name");
    params.put("mysql.matrix.primary.key.field.value", "1984");
    var mySqlConfig = MySqlConfig.of(params);
    Assertions.assertThrows(SQLException.class, () -> getConnection(mySqlConfig));
  }

  @Test
  void testBuildRhsQuery() {
    Map<String, Object> params = new HashMap<>();
    params.put("mysql.db.name", "db_name");
    params.put("mysql.rhs.table.name", "rhs_table");
    params.put("mysql.rhs.json.field.name", "json_filed");
    params.put("mysql.rhs.primary.key.field.name", "id");
    params.put("mysql.rhs.primary.key.field.value", "1984");
    var mySqlConfig = MySqlConfig.of(params);
    var actualQuery = buildRhsQuery(mySqlConfig);
    var expectedQuery = "SELECT json_filed FROM db_name.rhs_table WHERE id = ?";
    Assertions.assertEquals(expectedQuery, actualQuery);
  }

  @Test
  void testGetConnection() throws SQLException {
    Map<String, Object> params = new HashMap<>();
    params.put("mysql.url", "jdbc:h2:mem:db_name;DB_CLOSE_DELAY=-1");
    params.put("mysql.user", "test_user");
    params.put("mysql.password", "password");
    params.put("mysql.db.name", "db_name");
    params.put("mysql.rhs.table.name", "rhs_table");
    params.put("mysql.rhs.json.field.name", "json_filed");
    params.put("mysql.rhs.primary.key.field.name", "id");
    params.put("mysql.rhs.primary.key.field.value", "1984");
    var mySqlConfig = MySqlConfig.of(params);
    var mySqlConn = getConnection(mySqlConfig);
    Assertions.assertNotNull(mySqlConn);
  }

  @Test
  void testRequestMatrixData() throws SQLException, NoDataFoundException {
    Map<String, Object> params = new HashMap<>();
    params.put("mysql.url", "jdbc:h2:mem:db_name;DB_CLOSE_DELAY=-1");
    params.put("mysql.user", "test_user");
    params.put("mysql.password", "password");
    params.put("mysql.db.name", "db_name");
    params.put("mysql.matrix.table.name", "matrix_table");
    params.put("mysql.matrix.json.field.name", "json_field");
    params.put("mysql.matrix.primary.key.field.name", "id");
    params.put("mysql.matrix.primary.key.field.value", "1984");
    var mySqlConfig = MySqlConfig.of(params);
    var actualQuery = buildMatrixQuery(mySqlConfig);
    var mySqlConn = getConnection(mySqlConfig);
    var rawJson = requestMatrixData(mySqlConn, actualQuery, mySqlConfig);
    var actualMatrix = JsonHelper.fromJson(rawJson, double[][].class);
    var expectedMatrix = new double[][] {{3.4, 5.5, 2.2}, {1.2, 5.5, 8.1}, {3.4, 8.6, 9.4}};
    Assertions.assertArrayEquals(expectedMatrix, actualMatrix);
  }

  @Test
  void testRequestNoMatrixData() throws SQLException {
    Map<String, Object> params = new HashMap<>();
    params.put("mysql.url", "jdbc:h2:mem:db_name;DB_CLOSE_DELAY=-1");
    params.put("mysql.user", "test_user");
    params.put("mysql.password", "password");
    params.put("mysql.db.name", "db_name");
    params.put("mysql.matrix.table.name", "matrix_table");
    params.put("mysql.matrix.json.field.name", "json_field");
    params.put("mysql.matrix.primary.key.field.name", "id");
    params.put("mysql.matrix.primary.key.field.value", "78872");
    var mySqlConfig = MySqlConfig.of(params);
    var actualQuery = buildMatrixQuery(mySqlConfig);
    var mySqlConn = getConnection(mySqlConfig);
    Assertions.assertThrows(
        NoDataFoundException.class, () -> requestMatrixData(mySqlConn, actualQuery, mySqlConfig));
  }

  @Test
  void testExtractMatrix() {
    Map<String, Object> params = new HashMap<>();
    params.put("mysql.url", "jdbc:h2:mem:db_name;DB_CLOSE_DELAY=-1");
    params.put("mysql.user", "test_user");
    params.put("mysql.password", "password");
    params.put("mysql.db.name", "db_name");
    params.put("mysql.matrix.table.name", "matrix_table");
    params.put("mysql.matrix.json.field.name", "json_field");
    params.put("mysql.matrix.primary.key.field.name", "id");
    params.put("mysql.matrix.primary.key.field.value", "1984");
    var mySqlConfig = MySqlConfig.of(params);
    var actualMatrix = extractMatrix(mySqlConfig);
    var expectedMatrix = new double[][] {{3.4, 5.5, 2.2}, {1.2, 5.5, 8.1}, {3.4, 8.6, 9.4}};
    Assertions.assertArrayEquals(expectedMatrix, actualMatrix.get());
  }

  @Test
  void testExtractRhs() {
    Map<String, Object> params = new HashMap<>();
    params.put("mysql.url", "jdbc:h2:mem:db_name;DB_CLOSE_DELAY=-1");
    params.put("mysql.user", "test_user");
    params.put("mysql.password", "password");
    params.put("mysql.db.name", "db_name");
    params.put("mysql.rhs.table.name", "rhs_table");
    params.put("mysql.rhs.json.field.name", "json_field");
    params.put("mysql.rhs.primary.key.field.name", "id");
    params.put("mysql.rhs.primary.key.field.value", "1984");
    var mySqlConfig = MySqlConfig.of(params);
    var actualRhs = extractRhs(mySqlConfig);
    var expectedRhs = new double[] {4.7, 1.5, 5.3};
    Assertions.assertArrayEquals(expectedRhs, actualRhs.get());
  }

  @Test
  void testExtractNoRhsData() {
    Map<String, Object> params = new HashMap<>();
    params.put("mysql.url", "jdbc:h2:mem:db_name;DB_CLOSE_DELAY=-1");
    params.put("mysql.user", "test_user");
    params.put("mysql.password", "password");
    params.put("mysql.db.name", "db_name");
    params.put("mysql.rhs.table.name", "rhs_table");
    params.put("mysql.rhs.json.field.name", "json_field");
    params.put("mysql.rhs.primary.key.field.name", "id");
    params.put("mysql.rhs.primary.key.field.value", "762");
    var mySqlConfig = MySqlConfig.of(params);
    var query = buildRhsQuery(mySqlConfig);
    Assertions.assertThrows(
        NoDataFoundException.class, () -> requestRhsData(connection, query, mySqlConfig));
  }

  private static Connection getH2Connection() throws SQLException {
    var jdbcUrl = "jdbc:h2:mem:db_name;DB_CLOSE_DELAY=-1";
    var username = "test_user";
    var password = "password";
    return DriverManager.getConnection(jdbcUrl, username, password);
  }

  private static void initializeDatabase(Connection connection) throws SQLException {
    try (var stmt = connection.createStatement()) {
      stmt.execute("CREATE SCHEMA DB_NAME");
      stmt.execute("USE DB_NAME");
      stmt.execute("CREATE TABLE MATRIX_TABLE (ID INT PRIMARY KEY, JSON_FIELD VARCHAR(255))");
      stmt.execute("CREATE TABLE RHS_TABLE (ID INT PRIMARY KEY, JSON_FIELD VARCHAR(255))");
      stmt.execute(
          "INSERT INTO MATRIX_TABLE (ID, JSON_FIELD) VALUES (1984,"
              + " '[[3.4,5.5,2.2],[1.2,5.5,8.1],[3.4,8.6,9.4]]')");
      stmt.execute("INSERT INTO RHS_TABLE (ID, JSON_FIELD) VALUES (1984," + " '[4.7,1.5,5.3]')");
    }
  }
}

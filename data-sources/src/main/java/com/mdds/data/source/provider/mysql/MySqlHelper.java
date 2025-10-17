/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.mysql;

import com.mdds.api.Processable;
import com.mdds.common.util.JsonHelper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Helper class to get data from MySql database. */
public final class MySqlHelper {
  private MySqlHelper() {}

  public static Processable<double[][]> extractMatrix(MySqlConfig mySqlConfig) {
    try {
      return Processable.of(
          JsonHelper.fromJson(
              MySqlHelper.getRawMatrixDataFrom(
                  MySqlHelper.resuestData(
                      MySqlHelper.getConnection(mySqlConfig),
                      MySqlHelper.buildMatrixQuery(mySqlConfig)),
                  mySqlConfig),
              double[][].class));
    } catch (SQLException | NoDataFoundException e) {
      return Processable.failure("Error loading matrix from Db", e);
    }
  }

  public static Processable<double[]> extractRhs(MySqlConfig mySqlConfig) {
    try {
      return Processable.of(
          JsonHelper.fromJson(
              MySqlHelper.getRawRhsDataFrom(
                  MySqlHelper.resuestData(
                      MySqlHelper.getConnection(mySqlConfig),
                      MySqlHelper.buildRhsQuery(mySqlConfig)),
                  mySqlConfig),
              double[].class));
    } catch (SQLException | NoDataFoundException e) {
      return Processable.failure("Error loading right hand side from Db", e);
    }
  }

  public static Connection getConnection(MySqlConfig config) throws SQLException {
    return DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword());
  }

  public static String buildMatrixQuery(MySqlConfig config) {
    return "SELECT "
        + config.getMatrixJsonFieldName()
        + " FROM "
        + config.getDbName()
        + "."
        + config.getMatrixTableName()
        + " WHERE "
        + config.getMatrixJsonFieldName()
        + " = "
        + config.getMatrixPrimaryKeyFieldValue();
  }

  public static String buildRhsQuery(MySqlConfig config) {
    return "SELECT "
        + config.getRhsJsonFieldName()
        + " FROM "
        + config.getDbName()
        + "."
        + config.getRhsTableName()
        + " WHERE "
        + config.getRhsJsonFieldName()
        + " = "
        + config.getRhsPrimaryKeyFieldValue();
  }

  public static ResultSet resuestData(Connection connection, String query) throws SQLException {
    try (var statement = connection.createStatement()) {
      return statement.executeQuery(query);
    }
  }

  public static String getRawMatrixDataFrom(ResultSet resultSet, MySqlConfig config)
      throws SQLException, NoDataFoundException {
    if (resultSet.next()) {
      return resultSet.getString(config.getMatrixJsonFieldName());
    }
    throw new NoDataFoundException("No matrix data found for " + config);
  }

  public static String getRawRhsDataFrom(ResultSet resultSet, MySqlConfig config)
      throws SQLException, NoDataFoundException {
    if (resultSet.next()) {
      return resultSet.getString(config.getMatrixJsonFieldName());
    }
    throw new NoDataFoundException("No right hand side data found for " + config);
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.mysql;

import com.mdds.api.Processable;
import com.mdds.common.util.JsonHelper;
import jakarta.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Helper class to get data from MySql database. */
public final class MySqlHelper {
  private MySqlHelper() {}

  /**
   * Extracts matrix from MySql database.
   *
   * @param mySqlConfig configuration class for connecting to MqSql Db.
   * @return array of double values wrapped into <i>Processable</i> wrapper.
   */
  public static @Nonnull Processable<double[][]> extractMatrix(@Nonnull MySqlConfig mySqlConfig) {
    try {
      return Processable.of(
          JsonHelper.fromJson(
              requestMatrixData(
                  getConnection(mySqlConfig), buildMatrixQuery(mySqlConfig), mySqlConfig),
              double[][].class));
    } catch (SQLException | NoDataFoundException e) {
      return Processable.failure("Error loading matrix from Db", e);
    }
  }

  /**
   * Extracts right hand side vector from MySql database.
   *
   * @param mySqlConfig configuration class for connecting to MqSql Db.
   * @return array of double values wrapped into <i>Processable</i> wrapper.
   */
  public static @Nonnull Processable<double[]> extractRhs(@Nonnull MySqlConfig mySqlConfig) {
    try {
      return Processable.of(
          JsonHelper.fromJson(
              requestRhsData(getConnection(mySqlConfig), buildRhsQuery(mySqlConfig), mySqlConfig),
              double[].class));
    } catch (SQLException | NoDataFoundException e) {
      return Processable.failure("Error loading right hand side from Db", e);
    }
  }

  /**
   * Returns sql connection.
   *
   * @param config MySql configuration.
   * @return connection instance to MySql.
   * @throws SQLException when can not connect to MySql Database.
   */
  public static @Nonnull Connection getConnection(@Nonnull MySqlConfig config) throws SQLException {
    return DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword());
  }

  /**
   * Creates query to get matrix from database.
   *
   * @param config MySql connection configuration.
   * @return <i>String</i> query to select matrix from database.
   */
  public static @Nonnull String buildMatrixQuery(@Nonnull MySqlConfig config) {
    return "SELECT "
        + config.getMatrixJsonFieldName()
        + " FROM "
        + config.getDbName()
        + "."
        + config.getMatrixTableName()
        + " WHERE "
        + config.getMatrixPrimaryKeyFieldName()
        + " = ?";
  }

  /**
   * Creates query to get right hand side vector from database.
   *
   * @param config MySql connection configuration.
   * @return <i>String</i> query to select right hand side vector from database.
   */
  public static @Nonnull String buildRhsQuery(@Nonnull MySqlConfig config) {
    return "SELECT "
        + config.getRhsJsonFieldName()
        + " FROM "
        + config.getDbName()
        + "."
        + config.getRhsTableName()
        + " WHERE "
        + config.getRhsPrimaryKeyFieldName()
        + " = ?";
  }

  /**
   * Executes query and returns result set for right hand side vector query.
   *
   * @param connection connection to MySql Db.
   * @param query text of a query.
   * @param config MySql configuration
   * @return vector represented as Json in a <i>String</i> format.
   * @throws SQLException when can not execute a query.
   */
  public static @Nonnull String requestRhsData(
      @Nonnull Connection connection, @Nonnull String query, @Nonnull MySqlConfig config)
      throws SQLException, NoDataFoundException {
    try (var statement = connection.prepareStatement(query)) {
      statement.setString(1, config.getRhsPrimaryKeyFieldValue());
      var resultSet = statement.executeQuery();
      if (resultSet.next()) {
        return resultSet.getString(config.getRhsJsonFieldName());
      }
      throw new NoDataFoundException("No right hand side vector data found for " + config);
    }
  }

  /**
   * Executes query and returns result set for matrix query.
   *
   * @param connection connection to MySql Db.
   * @param query text of a query.
   * @param config MySql configuration
   * @return matrix represented as Json in a <i>String</i> format.
   * @throws SQLException when can not execute a query.
   */
  public static @Nonnull String requestMatrixData(
      @Nonnull Connection connection, @Nonnull String query, @Nonnull MySqlConfig config)
      throws SQLException, NoDataFoundException {
    try (var statement = connection.prepareStatement(query)) {
      statement.setString(1, config.getMatrixPrimaryKeyFieldValue());
      var resultSet = statement.executeQuery();
      if (resultSet.next()) {
        return resultSet.getString(config.getMatrixJsonFieldName());
      }
      throw new NoDataFoundException("No matrix data found for " + config);
    }
  }
}

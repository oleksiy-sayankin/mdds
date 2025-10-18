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

  /**
   * Extracts matrix from MySql database.
   *
   * @param mySqlConfig configuration class for connecting to MqSql Db.
   * @return array of double values wrapped into <i>Processable</i> wrapper.
   */
  public static Processable<double[][]> extractMatrix(MySqlConfig mySqlConfig) {
    try {
      return Processable.of(
          JsonHelper.fromJson(
              MySqlHelper.getRawMatrixDataFrom(
                  MySqlHelper.requestData(
                      MySqlHelper.getConnection(mySqlConfig),
                      MySqlHelper.buildMatrixQuery(mySqlConfig)),
                  mySqlConfig),
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
  public static Processable<double[]> extractRhs(MySqlConfig mySqlConfig) {
    try {
      return Processable.of(
          JsonHelper.fromJson(
              MySqlHelper.getRawRhsDataFrom(
                  MySqlHelper.requestData(
                      MySqlHelper.getConnection(mySqlConfig),
                      MySqlHelper.buildRhsQuery(mySqlConfig)),
                  mySqlConfig),
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
  public static Connection getConnection(MySqlConfig config) throws SQLException {
    return DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword());
  }

  /**
   * Creates query to get matrix from database.
   *
   * @param config MySql connection configuration.
   * @return <i>String</i> query to select matrix from database.
   */
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

  /**
   * Creates query to get right hand side vector from database.
   *
   * @param config MySql connection configuration.
   * @return <i>String</i> query to select right hand side vector from database.
   */
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

  /**
   * Executes query and returns result set of any query.
   *
   * @param connection connection to MySql Db.
   * @param query text of a query.
   * @return result set of any query.
   * @throws SQLException when can not execute a query.
   */
  public static ResultSet requestData(Connection connection, String query) throws SQLException {
    try (var statement = connection.createStatement()) {
      return statement.executeQuery(query);
    }
  }

  /**
   * Gets matrix as Json object that represented as <i>String</i>. We expect here that a matrix is
   * stored as simple string in database.
   *
   * @param resultSet data selected from database.
   * @param config configuration of connection to database.
   * @return matrix represented as Json in a <i>String</i> format.
   * @throws SQLException when we can not execute sql query.
   * @throws NoDataFoundException we throw it when there is no data in result set.
   */
  public static String getRawMatrixDataFrom(ResultSet resultSet, MySqlConfig config)
      throws SQLException, NoDataFoundException {
    if (resultSet.next()) {
      return resultSet.getString(config.getMatrixJsonFieldName());
    }
    throw new NoDataFoundException("No matrix data found for " + config);
  }

  /**
   * Gets right hand side vector of SLAE as Json object that represented as <i>String</i>. We expect
   * here that a vector is stored as simple string in database.
   *
   * @param resultSet data selected from database.
   * @param config configuration of connection to database.
   * @return vector represented as Json in a <i>String</i> format.
   * @throws SQLException when we can not execute sql query.
   * @throws NoDataFoundException we throw it when there is no data in result set.
   */
  public static String getRawRhsDataFrom(ResultSet resultSet, MySqlConfig config)
      throws SQLException, NoDataFoundException {
    if (resultSet.next()) {
      return resultSet.getString(config.getMatrixJsonFieldName());
    }
    throw new NoDataFoundException("No right hand side data found for " + config);
  }
}

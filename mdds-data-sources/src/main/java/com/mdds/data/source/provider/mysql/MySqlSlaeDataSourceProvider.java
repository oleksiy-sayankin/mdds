/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.mysql;

import static com.mdds.data.source.provider.mysql.MySqlHelper.extractMatrix;
import static com.mdds.data.source.provider.mysql.MySqlHelper.extractRhs;

import com.mdds.api.Processable;
import com.mdds.data.source.provider.SlaeDataSourceProvider;

/**
 * This data source provider reads matrix of coefficients and right hand side vector from MySql
 * database.
 */
public class MySqlSlaeDataSourceProvider implements SlaeDataSourceProvider {
  private final MySqlConfig mySqlConfig;

  public MySqlSlaeDataSourceProvider(MySqlConfig mySqlConfig) {
    this.mySqlConfig = mySqlConfig;
  }

  @Override
  public Processable<double[][]> loadMatrix() {
    return extractMatrix(mySqlConfig);
  }

  @Override
  public Processable<double[]> loadRhs() {
    return extractRhs(mySqlConfig);
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.http.request;

import static com.mdds.data.source.provider.http.request.HttpRequestHelper.extractMatrix;
import static com.mdds.data.source.provider.http.request.HttpRequestHelper.extractRhs;

import com.mdds.api.Processable;
import com.mdds.data.source.provider.SlaeDataSourceProvider;
import java.util.List;

/**
 * This data source provider reads matrix of coefficients and right hand side vector from instance
 * of configuration class.
 */
public class HttpRequestSlaeDataSourceProvider implements SlaeDataSourceProvider {
  private final List<? extends List<? extends Number>> rawMatrix;
  private final List<? extends Number> rawRhs;

  public HttpRequestSlaeDataSourceProvider(HttpRequestConfig config) {
    this.rawMatrix = config.getRawMatrix();
    this.rawRhs = config.getRawRhs();
  }

  @Override
  public Processable<double[][]> loadMatrix() {
    return extractMatrix(rawMatrix);
  }

  @Override
  public Processable<double[]> loadRhs() {
    return extractRhs(rawRhs);
  }
}

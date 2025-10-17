/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.http.request;

import static com.mdds.data.source.provider.http.request.HttpRequestHelper.extractMatrix;
import static com.mdds.data.source.provider.http.request.HttpRequestHelper.extractRhs;

import com.mdds.api.Processable;
import com.mdds.data.source.provider.SlaeDataSourceProvider;
import jakarta.servlet.http.HttpServletRequest;

/**
 * This data source provider reads matrix of coefficients and right hand side vector from instance
 * of multipart HttpServletRequest.
 */
public class HttpRequestSlaeDataSourceProvider implements SlaeDataSourceProvider {
  private final HttpServletRequest request;

  public HttpRequestSlaeDataSourceProvider(HttpRequestConfig config) {
    this.request = config.getRequest();
  }

  @Override
  public Processable<double[][]> loadMatrix() {
    return extractMatrix(request);
  }

  @Override
  public Processable<double[]> loadRhs() {
    return extractRhs(request);
  }
}

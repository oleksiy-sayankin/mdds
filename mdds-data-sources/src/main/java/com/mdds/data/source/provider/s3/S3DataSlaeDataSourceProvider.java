/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.s3;

import static com.mdds.data.source.provider.s3.S3Helper.extractMatrix;
import static com.mdds.data.source.provider.s3.S3Helper.extractRhs;

import com.mdds.api.Processable;
import com.mdds.data.source.provider.SlaeDataSourceProvider;

/**
 * This data source provider reads matrix of coefficients and right hand side vector from s3 bucket.
 */
public class S3DataSlaeDataSourceProvider implements SlaeDataSourceProvider {
  private final S3Config config;

  public S3DataSlaeDataSourceProvider(S3Config config) {
    this.config = config;
  }

  @Override
  public Processable<double[][]> loadMatrix() {
    return extractMatrix(config);
  }

  @Override
  public Processable<double[]> loadRhs() {
    return extractRhs(config);
  }
}

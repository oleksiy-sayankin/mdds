/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.http.request;

import com.mdds.api.Processable;
import jakarta.annotation.Nonnull;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/** Helper class for getting data from Http Request. */
@Slf4j
public final class HttpRequestHelper {
  private HttpRequestHelper() {}

  /**
   * Extracts matrix of the equation from the request.
   *
   * @param rawMatrix Http request.
   * @return matrix from the request.
   */
  public static Processable<double[][]> extractMatrix(
      @Nonnull List<? extends List<? extends Number>> rawMatrix) {
    if (rawMatrix.isEmpty()) {
      return Processable.failure("Matrix must not be empty");
    }
    var firstRow = rawMatrix.getFirst();
    var rows = rawMatrix.size();
    var cols = firstRow.size();
    if (cols == 0) {
      return Processable.failure("Matrix must have at least one column");
    }
    var result = new double[rows][cols];
    for (var i = 0; i <= rows - 1; i++) {
      var row = rawMatrix.get(i);
      if (row.size() != cols) {
        return Processable.failure("Matrix is not rectangular");
      }

      for (var j = 0; j <= cols - 1; j++) {
        var cell = row.get(j);
        result[i][j] = cell.doubleValue();
      }
    }

    return Processable.of(result);
  }

  /**
   * Extracts right hand side (rhs) vector of the equation from the request.
   *
   * @param rawRhs Http request.
   * @return right hand side vector from the request.
   */
  public static Processable<double[]> extractRhs(List<? extends Number> rawRhs) {
    var result = new double[rawRhs.size()];
    for (var i = 0; i <= rawRhs.size() - 1; i++) {
      var cell = rawRhs.get(i);
      result[i] = cell.doubleValue();
    }
    return Processable.of(result);
  }
}

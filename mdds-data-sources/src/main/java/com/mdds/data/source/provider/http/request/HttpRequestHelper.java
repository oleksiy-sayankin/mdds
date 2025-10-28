/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.http.request;

import static com.mdds.common.util.CsvHelper.*;

import com.mdds.api.Processable;
import com.opencsv.exceptions.CsvException;
import jakarta.annotation.Nonnull;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

/** Helper class for getting data from Http Request. */
@Slf4j
public final class HttpRequestHelper {
  private HttpRequestHelper() {}

  /**
   * Extracts matrix of the equation from the request.
   *
   * @param request Http request.
   * @return matrix from the request.
   */
  public static Processable<double[][]> extractMatrix(@Nonnull HttpServletRequest request) {
    try {
      var matrixPart = request.getPart("matrix");
      if (matrixPart == null) {
        return Processable.failure("Missing matrix in request");
      }
      try (var is = matrixPart.getInputStream()) {
        return Processable.of(convert(readCsvAsMatrix(is)));
      }

    } catch (IOException | ServletException | CsvException e) {
      return Processable.failure("Error parsing matrix in the request", e);
    }
  }

  /**
   * Extracts right hand side (rhs) vector of the equation from the request.
   *
   * @param request Http request.
   * @return right hand side vector from the request.
   */
  public static Processable<double[]> extractRhs(@Nonnull HttpServletRequest request) {
    try {
      var rhsPart = request.getPart("rhs");
      if (rhsPart == null) {
        return Processable.failure("Missing right hand side in request");
      }
      try (var is = rhsPart.getInputStream()) {
        return Processable.of(convert(readCsvAsVector(is)));
      }
    } catch (IOException | ServletException | CsvException e) {
      log.error("Error parsing right hand side in the request", e);
      return Processable.failure("Error parsing right hand side in the request", e);
    }
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.dto.SlaeSolver.isValid;
import static com.mdds.dto.SlaeSolver.parse;

import com.mdds.api.Processable;
import com.mdds.data.source.DataSourceDescriptor;
import com.mdds.dto.SlaeSolver;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** Helper class for processing REST API requests */
@Slf4j
public final class ServerHelper {

  private ServerHelper() {}

  /**
   * Extracts string containing solving method and converts it to enum variable.
   *
   * @param method SLAE solving method as raw string.
   * @return solving method.
   */
  public static Processable<SlaeSolver> extractSolvingMethod(String method) {
    if (!isValid(method)) {
      return Processable.failure("Invalid solving method: " + method);
    }
    return Processable.of(parse(method));
  }

  public static Processable<DataSourceDescriptor> extractDescriptor(
      String dataSourceType, Map<String, Object> params) {
    if (!DataSourceDescriptor.Type.isValid(dataSourceType)) {
      return Processable.failure("Invalid data source type: " + dataSourceType);
    }
    var type = DataSourceDescriptor.Type.parse(dataSourceType);
    if (params == null) {
      return Processable.failure("Parameters are null for data source type: " + dataSourceType);
    }
    if (!type.fitsFor(params)) {
      return Processable.failure(
          "The list of parameters is incomplete. Missing: " + type.findMissingIn(params));
    }
    return Processable.of(DataSourceDescriptor.of(type, params));
  }

  /**
   * Here we either return result from <i>Processable</i> wrapper or if we have no tesult, we raise
   * an exception for quick exist from method.
   *
   * @param result <i>Processable</i> wrapper which may contain the result or error description.
   * @return unwrapped data from <i>Processable</i> wrapper.
   * @param <T> type of data that <i>Processable</i> wraps.
   */
  public static <T> T unwrapOrSendError(Processable<T> result) {
    if (result.isFailure()) {
      throw new EarlyExitException(result.getErrorMessage());
    }
    return result.get();
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.dto.SlaeSolver.isValid;
import static com.mdds.dto.SlaeSolver.parse;
import static com.mdds.server.ServerAppContextListener.ATTR_SERVER_SERVICE;

import com.mdds.api.Processable;
import com.mdds.common.util.JsonHelper;
import com.mdds.data.source.DataSourceDescriptor;
import com.mdds.dto.SlaeSolver;
import com.mdds.queue.Queue;
import com.mdds.storage.DataStorage;
import jakarta.annotation.Nonnull;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ServletHelper {

  private ServletHelper() {}

  /**
   * Gets Queue instance from the request.
   *
   * @param servletContext Http request.
   * @return Queue instance.
   */
  public static Processable<Queue> extractQueue(@Nonnull ServletContext servletContext) {
    var service = servletContext.getAttribute(ATTR_SERVER_SERVICE);
    if (service == null) {
      return Processable.failure(ATTR_ERROR);
    }
    return Processable.of(((ServerService) service).getQueue());
  }

  /**
   * Extracts string containing solving method and converts it to enum variable.
   *
   * @param request Http request.
   * @return solving method.
   */
  public static Processable<SlaeSolver> extractSolvingMethod(@Nonnull HttpServletRequest request) {
    var method = request.getParameter("slaeSolvingMethod");
    if (!isValid(method)) {
      return Processable.failure("Invalid solving method: " + method);
    }
    return Processable.of(parse(method));
  }

  public static Processable<DataSourceDescriptor> extractDescriptor(
      @Nonnull HttpServletRequest request) {
    var dataSourceType = request.getParameter("dataSourceType");
    if (!DataSourceDescriptor.Type.isValid(dataSourceType)) {
      return Processable.failure("Invalid data source type: " + dataSourceType);
    }
    var params = new HashMap<String, Object>();
    var type = DataSourceDescriptor.Type.parse(dataSourceType);
    if (DataSourceDescriptor.Type.HTTP_REQUEST.equals(type)) {
      params.put("request", request);
    }
    return Processable.of(DataSourceDescriptor.of(type, params));
  }

  /**
   * Sends error to the Http response.
   *
   * @param response response object.
   * @param sc status code
   * @param message message for the staus code.
   */
  public static void sendError(@Nonnull HttpServletResponse response, int sc, String message) {
    try {
      response.sendError(sc, message);
    } catch (IOException e) {
      log.error(message, e);
    }
  }

  /**
   * Write Json object to Http response.
   *
   * @param response Http response where to write data.
   * @param result object to be written as Json.
   */
  public static void writeJson(@Nonnull HttpServletResponse response, Object result) {
    try {
      response.getWriter().write(JsonHelper.toJson(result));
    } catch (IOException e) {
      log.error("Error writing task id as JSON", e);
    }
  }

  /**
   * Writes error message that a task with a certain id does not exist.
   *
   * @param response Http response.
   */
  public static void writeNotFound(@Nonnull HttpServletResponse response) {
    try {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      response.getWriter().write("{\"error\":\"no_result_for_provided_task_id\"}");
    } catch (IOException e) {
      log.error("Error writing not-found response", e);
    }
  }

  /**
   * Gets DataStorage instance from the request.
   *
   * @param servletContext Http request.
   * @return DataStorage instance.
   */
  public static Processable<DataStorage> extractDataStorage(
      @Nonnull ServletContext servletContext) {
    var service = servletContext.getAttribute(ATTR_SERVER_SERVICE);
    if (service == null) {
      return Processable.failure(ATTR_ERROR);
    }
    return Processable.of(((ServerService) service).getDataStorage());
  }

  /**
   * Extracts task id from the request.
   *
   * @param request Http request.
   * @return task id from the request.
   */
  public static Processable<String> extractTaskId(@Nonnull HttpServletRequest request) {
    var path = request.getPathInfo();
    if (path == null || path.isEmpty()) {
      return Processable.failure("Path is empty");
    }
    var taskId = path.startsWith("/") ? path.substring(path.lastIndexOf('/') + 1) : null;
    if (taskId == null || taskId.isEmpty()) {
      return Processable.failure("Task id missing");
    }
    return Processable.of(taskId);
  }

  /** Stub exception to perform early exit from set of operations. */
  public static class EarlyExitException extends RuntimeException {}

  /**
   * Here we either return result from <i>Processable</i> wrapper or if we have no tesult, we send
   * error message to http servlet response instance.
   *
   * @param response http servlet response instance where to send error message.
   * @param statusCode statis code to send
   * @param result <i>Processable</i> wrapper which may contain the result or error description.
   * @return unwrapped data from <i>Processable</i> wrapper.
   * @param <T> type of data that <i>Processable</i> wraps.
   */
  public static <T> T unwrapOrSendError(
      HttpServletResponse response, int statusCode, Processable<T> result) {
    if (result.isFailure()) {
      sendError(response, statusCode, result.getErrorMessage());
      throw new EarlyExitException();
    }
    return result.get();
  }

  private static final String ATTR_ERROR =
      "Servlet context attribute " + ATTR_SERVER_SERVICE + " is null.";
}

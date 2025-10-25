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
import java.util.Map;
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
    var type = DataSourceDescriptor.Type.parse(dataSourceType);
    var params = fillInParamsFrom(type, request);
    return Processable.of(DataSourceDescriptor.of(type, params));
  }

  /**
   * Reads parameters from Http servlet request depending on the datasource descriptor type.
   *
   * @param type datasource descriptor type.
   * @param request Http servlet request with parameters.
   * @return Map that contains parameters read from Http servlet request.
   */
  public static Map<String, Object> fillInParamsFrom(
      @Nonnull DataSourceDescriptor.Type type, @Nonnull HttpServletRequest request) {
    var params = new HashMap<String, Object>();
    switch (type) {
      case HTTP_REQUEST:
        params.put("request", request);
        return params;
      case MYSQL:
        params.put("mysql.url", request.getParameter("mysqlUrl"));
        params.put("mysql.user", request.getParameter("mysqlUser"));
        params.put("mysql.password", request.getParameter("mysqlPassword"));
        params.put("mysql.db.name", request.getParameter("mysqlDbName"));
        params.put("mysql.matrix.table.name", request.getParameter("mysqlMatrixTableName"));
        params.put(
            "mysql.matrix.json.field.name", request.getParameter("mysqlMatrixJsonFieldName"));
        params.put(
            "mysql.matrix.primary.key.field.name",
            request.getParameter("mysqlMatrixPrimaryKeyFieldName"));
        params.put(
            "mysql.matrix.primary.key.field.value",
            request.getParameter("mysqlMatrixPrimaryKeyFieldValue"));
        params.put("mysql.rhs.table.name", request.getParameter("mysqlRhsTableName"));
        params.put("mysql.rhs.json.field.name", request.getParameter("mysqlRhsJsonFieldName"));
        params.put(
            "mysql.rhs.primary.key.field.name",
            request.getParameter("mysqlRhsPrimaryKeyFieldName"));
        params.put(
            "mysql.rhs.primary.key.field.value",
            request.getParameter("mysqlRhsPrimaryKeyFieldValue"));
        return params;
      case S3:
        params.put("aws.bucket.name", request.getParameter("awsBucketName"));
        params.put("aws.use.endpoint.url", request.getParameter("awsUseEndPointUrl"));
        params.put("aws.endpoint.url", request.getParameter("awsEndPointUrl"));
        params.put("aws.region", request.getParameter("awsRegion"));
        params.put("aws.access.key.id", request.getParameter("awsAccessKeyId"));
        params.put("aws.secret.access.key", request.getParameter("awsSecretAccessKey"));
        params.put("aws.matrix.key", request.getParameter("awsMatrixKey"));
        params.put("aws.rhs.key", request.getParameter("awsRhsKey"));
        params.put(
            "aws.path.style.access.enabled", request.getParameter("awsPathStyleAccessEnabled"));
        return params;
    }
    return params;
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

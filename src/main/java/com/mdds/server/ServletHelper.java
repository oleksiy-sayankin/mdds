/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.server.CsvHelper.*;
import static dto.SlaeSolver.isValid;
import static dto.SlaeSolver.parse;
import static jakarta.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static jakarta.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;

import com.mdds.queue.Queue;
import com.mdds.storage.DataStorage;
import com.mdds.util.JsonHelper;
import com.opencsv.exceptions.CsvException;
import dto.SlaeSolver;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ServletHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServletHelper.class);

  private ServletHelper() {}

  public static Optional<Queue> extractQueue(
      HttpServletRequest request, HttpServletResponse response) {
    var ctx = request.getServletContext();
    var taskQueue = (Queue) ctx.getAttribute(AppContextListener.ATTR_TASK_QUEUE);
    if (taskQueue == null) {
      sendError(response, SC_INTERNAL_SERVER_ERROR, "Service Queue is not initialized");
      return Optional.empty();
    }
    return Optional.of(taskQueue);
  }

  public static Optional<DataStorage> extractDataStorage(
      HttpServletRequest request, HttpServletResponse response) {
    var ctx = request.getServletContext();
    var storage = (DataStorage) ctx.getAttribute(AppContextListener.ATTR_DATA_STORAGE);
    if (storage == null) {
      sendError(response, SC_INTERNAL_SERVER_ERROR, "Service Data Storage is not initialized");
      return Optional.empty();
    }
    return Optional.of(storage);
  }

  public static Optional<SlaeSolver> extractSolvingMethod(
      HttpServletRequest request, HttpServletResponse response) {
    var method = request.getParameter("slaeSolvingMethod");
    if (!isValid(method)) {
      sendError(response, SC_BAD_REQUEST, "Invalid solving method: " + method);
      return Optional.empty();
    }
    return Optional.of(parse(method));
  }

  /**
   * Sends error to the Http response.
   *
   * @param response response object.
   * @param sc status code
   * @param message message for the staus code.
   */
  public static void sendError(HttpServletResponse response, int sc, String message) {
    try {
      response.sendError(sc, message);
    } catch (IOException e) {
      LOGGER.error(message, e);
    }
  }

  public static void writeJson(HttpServletResponse response, Object result) {
    try {
      response.getWriter().write(JsonHelper.toJson(result));
    } catch (IOException e) {
      LOGGER.error("Error writing task id as JSON", e);
    }
  }

  public static Optional<double[][]> extractMatrix(
      HttpServletRequest request, HttpServletResponse response) {
    try {
      var matrixPart = request.getPart("matrix");
      if (matrixPart == null) {
        sendError(response, SC_BAD_REQUEST, "Missing matrix in request");
        return Optional.empty();
      }
      try (var is = matrixPart.getInputStream()) {
        return Optional.of(convert(readCsvAsMatrix(is)));
      }

    } catch (IOException | ServletException | CsvException | SolveServletException e) {
      LOGGER.error("Error parsing matrix in the request", e);
      return Optional.empty();
    }
  }

  public static Optional<double[]> extractRhs(
      HttpServletRequest request, HttpServletResponse response) {
    try {
      var rhsPart = request.getPart("rhs");
      if (rhsPart == null) {
        sendError(response, SC_BAD_REQUEST, "Missing right hand side in request");
        return Optional.empty();
      }
      try (var is = rhsPart.getInputStream()) {
        return Optional.of(convert(readCsvAsVector(is)));
      }
    } catch (IOException | ServletException | CsvException | SolveServletException e) {
      LOGGER.error("Error parsing right hand side in the request", e);
      return Optional.empty();
    }
  }

  public static void writeNotFound(HttpServletResponse response) {
    try {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      response.getWriter().write("{\"error\":\"no_result_for_provided_task_id\"}");
    } catch (IOException e) {
      LOGGER.error("Error writing not-found response", e);
    }
  }

  public static Optional<DataStorage> getStorage(
      HttpServletRequest request, HttpServletResponse response) {
    return Optional.ofNullable(
            (DataStorage)
                request.getServletContext().getAttribute(AppContextListener.ATTR_DATA_STORAGE))
        .or(
            () -> {
              sendError(
                  response, SC_INTERNAL_SERVER_ERROR, "Data Storage Service is not initialized");
              return Optional.empty();
            });
  }

  public static Optional<String> extractTaskId(
      HttpServletRequest request, HttpServletResponse response) {
    var path = request.getPathInfo();
    if (path == null || path.isEmpty()) {
      sendError(response, SC_BAD_REQUEST, "Path is empty");
      return Optional.empty();
    }
    var taskId = path.startsWith("/") ? path.substring(path.lastIndexOf('/') + 1) : null;
    if (taskId == null || taskId.isEmpty()) {
      sendError(response, SC_BAD_REQUEST, "Task id missing");
      return Optional.empty();
    }
    return Optional.of(taskId);
  }
}

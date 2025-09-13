/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.storage.DataStorage;
import com.mdds.util.JsonHelper;
import dto.ResultDTO;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Get result as document from key-value data storage. */
@WebServlet(
    name = "resultServlet",
    urlPatterns = {"/result/*"})
public class ResultServlet extends HttpServlet {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResultServlet.class);

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    LOGGER.info("Processing request in result servlet...");
    response.setContentType("application/json");
    var taskId = extractTaskId(request, response);
    if (taskId.isEmpty()) {
      return;
    }
    var storage = getStorage(request, response);
    if (storage.isEmpty()) {
      return;
    }
    var result = storage.get().get(taskId.get(), ResultDTO.class);
    if (result.isEmpty()) {
      writeNotFound(response);
      return;
    }
    writeJson(response, result.get());
  }

  private static Optional<String> extractTaskId(
      HttpServletRequest request, HttpServletResponse response) {
    var path = request.getPathInfo();
    if (path == null || path.isEmpty()) {
      sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Path is empty");
      return Optional.empty();
    }
    var taskId = path.startsWith("/") ? path.substring(path.lastIndexOf('/') + 1) : null;
    if (taskId == null || taskId.isEmpty()) {
      sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Task id missing");
      return Optional.empty();
    }
    return Optional.of(taskId);
  }

  private static Optional<DataStorage> getStorage(
      HttpServletRequest request, HttpServletResponse response) {
    return Optional.ofNullable(
            (DataStorage)
                request.getServletContext().getAttribute(AppContextListener.ATTR_DATA_STORAGE))
        .or(
            () -> {
              sendError(
                  response,
                  HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                  "Data Storage Service is not initialized");
              return Optional.empty();
            });
  }

  private static void writeNotFound(HttpServletResponse response) {
    try {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      response.getWriter().write("{\"error\":\"no_result_for_provided_task_id\"}");
    } catch (IOException e) {
      LOGGER.error("Error writing not-found response", e);
    }
  }

  private static void writeJson(HttpServletResponse response, ResultDTO result) {
    try {
      response.getWriter().write(JsonHelper.toJson(result));
    } catch (IOException e) {
      LOGGER.error("Error writing result as JSON", e);
    }
  }

  private static void sendError(HttpServletResponse response, int status, String message) {
    try {
      response.sendError(status, message);
    } catch (IOException e) {
      LOGGER.error(message, e);
    }
  }
}

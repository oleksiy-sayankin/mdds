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
    response.setContentType("application/json");
    String taskId = extractTaskId(request, response);
    if (taskId == null) {
      return;
    }
    DataStorage storage = getStorage(request, response);
    if (storage == null) {
      return;
    }
    ResultDTO result = storage.get(taskId, ResultDTO.class);
    if (result == null) {
      writeNotFound(response);
      return;
    }
    writeJson(response, result);
  }

  private static String extractTaskId(HttpServletRequest request, HttpServletResponse response) {
    String path = request.getPathInfo();
    if (path == null || path.isEmpty()) {
      sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Path is empty");
      return null;
    }
    String taskId = path.startsWith("/") ? path.substring(path.lastIndexOf('/') + 1) : null;
    if (taskId == null || taskId.isEmpty()) {
      sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Task id missing");
      return null;
    }
    return taskId;
  }

  private static DataStorage getStorage(HttpServletRequest request, HttpServletResponse response) {
    DataStorage storage =
        (DataStorage)
            request.getServletContext().getAttribute(AppContextListener.ATTR_DATA_STORAGE);
    if (storage == null) {
      sendError(
          response,
          HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          "Data Storage Service is not initialized");
    }
    return storage;
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

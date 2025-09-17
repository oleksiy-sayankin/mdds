/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import dto.ResultDTO;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mdds.server.ServletHelper.*;

/** Get result as document from key-value data storage. */
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
    var storage = extractDataStorage(request, response);
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
}

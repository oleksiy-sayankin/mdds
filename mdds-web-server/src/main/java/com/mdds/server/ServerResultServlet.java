/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.server.ServletHelper.*;
import static jakarta.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static jakarta.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;

import com.mdds.dto.ResultDTO;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;

/** Get result as document from key-value data storage. */
@Slf4j
public class ServerResultServlet extends HttpServlet {

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    log.info("Processing request in result servlet...");
    try {
      var taskId = unwrapOrSendError(response, SC_BAD_REQUEST, extractTaskId(request));
      var storage =
          unwrapOrSendError(
              response, SC_INTERNAL_SERVER_ERROR, extractDataStorage(getServletContext()));
      var result = storage.get(taskId, ResultDTO.class);
      response.setContentType("application/json");
      result.ifPresentOrElse(r -> writeJson(response, r), () -> writeNotFound(response));
    } catch (EarlyExitException ignore) {
      // Exit if there is at least one error in unwrapping data
    }
  }
}

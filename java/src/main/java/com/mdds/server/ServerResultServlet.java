/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.server.ServletHelper.*;

import dto.ResultDTO;
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
    response.setContentType("application/json");
    var taskId = extractTaskId(request, response);
    var storage = extractDataStorage(request, response);

    storage.ifPresent(
        s ->
            taskId.ifPresent(
                t -> {
                  var result = s.get(t, ResultDTO.class);
                  result.ifPresentOrElse(
                      r -> writeJson(response, r), () -> writeNotFound(response));
                }));
  }
}

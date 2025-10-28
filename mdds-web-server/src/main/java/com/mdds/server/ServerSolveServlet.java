/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.common.AppConstants.TASK_QUEUE_NAME;
import static com.mdds.data.source.DataSourceProviderFactory.fromDescriptor;
import static com.mdds.server.ServletHelper.*;
import static jakarta.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static jakarta.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;

import com.mdds.common.AppConstantsFactory;
import com.mdds.dto.*;
import com.mdds.queue.Message;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.time.Instant;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

/** Servlet for solving system of linear algebraic equations. */
@Slf4j
public class ServerSolveServlet extends HttpServlet {

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    log.info("Processing request in Solve Servlet...");
    try {
      var descriptor = unwrapOrSendError(response, SC_BAD_REQUEST, extractDescriptor(request));
      var provider = unwrapOrSendError(response, SC_BAD_REQUEST, fromDescriptor(descriptor));
      var matrix = unwrapOrSendError(response, SC_BAD_REQUEST, provider.loadMatrix());
      var rhs = unwrapOrSendError(response, SC_BAD_REQUEST, provider.loadRhs());
      var method = unwrapOrSendError(response, SC_BAD_REQUEST, extractSolvingMethod(request));
      var storage =
          unwrapOrSendError(
              response, SC_INTERNAL_SERVER_ERROR, extractDataStorage(getServletContext()));
      var queue =
          unwrapOrSendError(response, SC_INTERNAL_SERVER_ERROR, extractQueue(getServletContext()));

      // store initial result
      var taskId = UUID.randomUUID().toString();
      var now = Instant.now();
      // Put result to storage
      storage.put(taskId, new ResultDTO(taskId, now, null, TaskStatus.IN_PROGRESS, null, null));

      // create TaskDTO and publish to queue
      queue.publish(
          AppConstantsFactory.getString(TASK_QUEUE_NAME),
          new Message<>(
              new TaskDTO(taskId, now, matrix, rhs, method), Collections.emptyMap(), now));
      response.setContentType("application/json");
      writeJson(response, new TaskIdResponseDTO(taskId));
    } catch (EarlyExitException ignored) {
      // Exit if there is at least one error in unwrapping data
    }
  }
}

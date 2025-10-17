/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.common.AppConstants.TASK_QUEUE_NAME;
import static com.mdds.server.ServletHelper.*;
import static jakarta.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static jakarta.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;

import com.mdds.common.AppConstantsFactory;
import com.mdds.data.source.DataSourceProviderFactory;
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
    var dataSourceDescriptor = extractDataSourceDescriptor(request);
    if (dataSourceDescriptor.isFailure()) {
      sendError(response, SC_BAD_REQUEST, dataSourceDescriptor.getErrorMessage());
      return;
    }
    var slaeDataSourceProvider =
        DataSourceProviderFactory.fromDescriptor(dataSourceDescriptor.get());
    if (slaeDataSourceProvider.isFailure()) {
      sendError(response, SC_BAD_REQUEST, slaeDataSourceProvider.getErrorMessage());
      return;
    }
    var matrix = slaeDataSourceProvider.get().loadMatrix();
    if (matrix.isFailure()) {
      sendError(response, SC_BAD_REQUEST, matrix.getErrorMessage());
      return;
    }
    var rhs = slaeDataSourceProvider.get().loadRhs();
    if (rhs.isFailure()) {
      sendError(response, SC_BAD_REQUEST, rhs.getErrorMessage());
      return;
    }
    var method = extractSolvingMethod(request);
    if (method.isFailure()) {
      sendError(response, SC_BAD_REQUEST, method.getErrorMessage());
      return;
    }
    var servletContext = getServletContext();
    var storage = extractDataStorage(servletContext);
    if (storage.isFailure()) {
      sendError(response, SC_INTERNAL_SERVER_ERROR, storage.getErrorMessage());
      return;
    }
    var queue = extractQueue(servletContext);
    if (queue.isFailure()) {
      sendError(response, SC_INTERNAL_SERVER_ERROR, queue.getErrorMessage());
      return;
    }

    // store initial result
    var taskId = UUID.randomUUID().toString();
    var now = Instant.now();
    // Put result to storage
    storage.ifPresent(
        s -> s.put(taskId, new ResultDTO(taskId, now, null, TaskStatus.IN_PROGRESS, null, null)));

    // create TaskDTO and publish to queue
    queue.ifPresent(
        q ->
            q.publish(
                AppConstantsFactory.getString(TASK_QUEUE_NAME),
                new Message<>(
                    new TaskDTO(taskId, now, matrix.get(), rhs.get(), method.get()),
                    Collections.emptyMap(),
                    now)));
    response.setContentType("application/json");
    writeJson(response, new TaskIdResponseDTO(taskId));
  }
}

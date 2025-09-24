/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.common.AppConstants.TASK_QUEUE_NAME;
import static com.mdds.server.ServletHelper.*;

import com.mdds.common.AppConstantsFactory;
import com.mdds.queue.Message;
import dto.*;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Servlet for solving system of linear algebraic equations. */
public class ServerSolveServlet extends HttpServlet {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerSolveServlet.class);

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    LOGGER.info("Processing request in Solve Servlet...");
    var matrix = extractMatrix(request, response);
    var method = extractSolvingMethod(request, response);
    var rhs = extractRhs(request, response);
    var storage = extractDataStorage(request, response);
    var queue = extractQueue(request, response);

    if (Stream.of(matrix, method, rhs, storage, queue).anyMatch(Optional::isEmpty)) {
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
                    new TaskDTO(
                        taskId, now, matrix.orElseThrow(), rhs.orElseThrow(), method.orElseThrow()),
                    Collections.emptyMap(),
                    now)));
    response.setContentType("application/json");
    writeJson(response, new TaskIdResponseDTO(taskId));
  }
}

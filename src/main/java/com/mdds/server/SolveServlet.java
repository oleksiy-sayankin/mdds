/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.server.ServletHelper.*;

import com.mdds.queue.Message;
import dto.*;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Servlet for solving system of linear algebraic equations. */
@WebServlet(
    name = "solveServlet",
    urlPatterns = {"/solve"})
public class SolveServlet extends HttpServlet {
  private static final Logger LOGGER = LoggerFactory.getLogger(SolveServlet.class);
  private static final String TASK_QUEUE_NAME = "task_queue";

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
    var result = new ResultDTO(taskId, now, null, TaskStatus.IN_PROGRESS, null, null);
    storage.get().put(taskId, result);

    // create TaskDTO and publish to queue
    var task = new TaskDTO(taskId, now, matrix.get(), rhs.get(), method.get());
    var message = new Message<>(task, Collections.emptyMap(), now);
    queue.get().publish(TASK_QUEUE_NAME, message);
    response.setContentType("application/json");
    writeJson(response, new TaskIdResponseDTO(taskId));
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static dto.SlaeSolver.isValid;
import static dto.SlaeSolver.parse;

import com.mdds.queue.Message;
import com.mdds.queue.Queue;
import com.mdds.storage.DataStorage;
import com.mdds.util.JsonHelper;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import dto.*;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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

  private static Optional<Queue> extractQueue(
      HttpServletRequest request, HttpServletResponse response) {
    var ctx = request.getServletContext();
    var taskQueue = (Queue) ctx.getAttribute(AppContextListener.ATTR_TASK_QUEUE);
    if (taskQueue == null) {
      sendError(response, "Service Queue is not initialized");
      return Optional.empty();
    }
    return Optional.of(taskQueue);
  }

  private static Optional<DataStorage> extractDataStorage(
      HttpServletRequest request, HttpServletResponse response) {
    var ctx = request.getServletContext();
    var storage = (DataStorage) ctx.getAttribute(AppContextListener.ATTR_DATA_STORAGE);
    if (storage == null) {
      sendError(response, "Service Data Storage is not initialized");
      return Optional.empty();
    }
    return Optional.of(storage);
  }

  private static Optional<SlaeSolver> extractSolvingMethod(
      HttpServletRequest request, HttpServletResponse response) {
    var method = request.getParameter("slaeSolvingMethod");
    if (!isValid(method)) {
      sendError(response, "Invalid solving method: " + method);
      return Optional.empty();
    }
    return Optional.of(parse(method));
  }

  private static void sendError(HttpServletResponse response, String message) {
    try {
      response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, message);
    } catch (IOException e) {
      LOGGER.error(message, e);
    }
  }

  private static void writeJson(HttpServletResponse response, Object result) {
    try {
      response.getWriter().write(JsonHelper.toJson(result));
    } catch (IOException e) {
      LOGGER.error("Error writing task id as JSON", e);
    }
  }

  private static Optional<double[][]> extractMatrix(
      HttpServletRequest request, HttpServletResponse response) {
    try {
      var matrixPart = request.getPart("matrix");
      if (matrixPart == null) {
        sendError(response, "Missing matrix in request");
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

  private static Optional<double[]> extractRhs(
      HttpServletRequest request, HttpServletResponse response) {
    try {
      var rhsPart = request.getPart("rhs");
      if (rhsPart == null) {
        sendError(response, "Missing right hand side in request");
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

  private static double[] convert(String[] rhs) throws SolveServletException {
    var result = new double[rhs.length];
    var i = 0;
    for (var number : rhs) {
      try {
        result[i] = Double.parseDouble(number);
      } catch (NumberFormatException e) {
        throw new SolveServletException("Can not convert to double: " + number, e);
      }
      i++;
    }
    return result;
  }

  private static double[][] convert(String[][] matrix) {
    var result = new double[matrix.length][];
    var i = 0;
    for (var row : matrix) {
      result[i] = new double[matrix[i].length];
      var j = 0;
      for (var number : row) {
        try {
          result[i][j] = Double.parseDouble(number);
        } catch (NumberFormatException e) {
          throw new SolveServletException("Can not convert to double: " + number, e);
        }
        j++;
      }
      i++;
    }
    return result;
  }

  private static String[] readCsvAsVector(InputStream inputStream)
      throws IOException, CsvException {
    try (var reader = new BufferedReader(new InputStreamReader(inputStream));
        var csvReader = new CSVReader(reader)) {
      var allData = csvReader.readAll();
      var rhs = new String[allData.size()];
      var i = 0;
      for (var row : allData) {
        rhs[i] = row[0];
        i++;
      }
      return rhs;
    }
  }

  private static String[][] readCsvAsMatrix(InputStream inputStream)
      throws IOException, CsvException {
    try (var reader = new BufferedReader(new InputStreamReader(inputStream));
        var csvReader = new CSVReader(reader)) {
      var allData = csvReader.readAll();
      return allData.toArray(new String[0][0]);
    }
  }
}

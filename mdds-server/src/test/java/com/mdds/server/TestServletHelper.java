/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.github.valfirst.slf4jtest.TestLoggerFactory.getTestLogger;
import static com.mdds.server.CsvHelper.convert;
import static com.mdds.server.ServletHelper.*;
import static jakarta.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static jakarta.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.mdds.dto.SlaeSolver;
import com.mdds.queue.Queue;
import com.mdds.storage.DataStorage;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.Part;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestServletHelper {
  private HttpServletRequest request;
  private HttpServletResponse response;
  private ServletContext servletContext;
  private Queue queue;
  private DataStorage dataStorage;
  private ServerService serverService;

  @BeforeEach
  void setUp() {
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
    servletContext = mock(ServletContext.class);
    queue = mock(Queue.class);
    dataStorage = mock(DataStorage.class);
    serverService = mock(ServerService.class);
  }

  @Test
  void testExtractQueueWhenQueueExists() {
    when(request.getServletContext()).thenReturn(servletContext);
    when(servletContext.getAttribute(ServerAppContextListener.ATTR_SERVER_SERVICE))
        .thenReturn(serverService);
    when(serverService.getQueue()).thenReturn(queue);
    var actualQueue = extractQueue(request, response);
    actualQueue.ifPresent(q -> assertEquals(queue, q));
  }

  @Test
  void testExtractQueueWhenNoQueue() throws IOException {
    when(request.getServletContext()).thenReturn(servletContext);
    when(servletContext.getAttribute(ServerAppContextListener.ATTR_SERVER_SERVICE))
        .thenReturn(serverService);
    var actualQueue = extractQueue(request, response);
    verify(response).sendError(SC_INTERNAL_SERVER_ERROR, "Service Queue is not initialized");
    assertTrue(actualQueue.isEmpty());
  }

  @Test
  void testExtractDataStorageWhenDataStorageExists() {
    when(request.getServletContext()).thenReturn(servletContext);
    when(servletContext.getAttribute(ServerAppContextListener.ATTR_SERVER_SERVICE))
        .thenReturn(serverService);
    when(serverService.getDataStorage()).thenReturn(dataStorage);
    var actualDataStorage = extractDataStorage(request, response);
    actualDataStorage.ifPresent(ds -> assertEquals(dataStorage, ds));
  }

  @Test
  void testExtractSolvingMethodWhenMethodExists() {
    when(request.getParameter("slaeSolvingMethod")).thenReturn("numpy_exact_solver");
    var actualSolvingMethod = extractSolvingMethod(request, response);
    actualSolvingMethod.ifPresent(asm -> assertEquals(SlaeSolver.NUMPY_EXACT_SOLVER, asm));
  }

  @Test
  void testExtractSolvingMethodWhenWrongMethod() throws IOException {
    var solvingMethod = "wrong_solving_method";
    when(request.getParameter("slaeSolvingMethod")).thenReturn(solvingMethod);
    var actualSolvingMethod = extractSolvingMethod(request, response);
    verify(response).sendError(SC_BAD_REQUEST, "Invalid solving method: " + solvingMethod);
    assertTrue(actualSolvingMethod.isEmpty());
  }

  @Test
  void testExtractSolvingMethodWhenNullMethod() throws IOException {
    when(request.getParameter("slaeSolvingMethod")).thenReturn(null);
    var actualSolvingMethod = extractSolvingMethod(request, response);
    verify(response).sendError(SC_BAD_REQUEST, "Invalid solving method: " + null);
    assertTrue(actualSolvingMethod.isEmpty());
  }

  @Test
  void testSendError() throws IOException {
    var message = "Test error message";
    doNothing().when(response).sendError(anyInt(), anyString());
    sendError(response, SC_BAD_REQUEST, message);
    verify(response).sendError(SC_BAD_REQUEST, message);
  }

  @Test
  void testSendErrorAndLogException() throws IOException {
    var logger = getTestLogger(ServletHelper.class);
    var message = "Test error message";
    doThrow(new IOException()).when(response).sendError(anyInt(), anyString());
    sendError(response, SC_BAD_REQUEST, message);
    verify(response).sendError(SC_BAD_REQUEST, message);
    assertTrue(logger.getLoggingEvents().stream().anyMatch(e -> e.getMessage().contains(message)));
  }

  @Test
  void testExtractMatrix() throws ServletException, IOException {
    var expectedMatrix =
        new String[][] {{"1.3", "2.2", "3.7"}, {"7.7", "2.1", "9.3"}, {"1.1", "4.8", "2.3"}};
    var sb = new StringBuilder();
    for (var row : expectedMatrix) {
      sb.append(String.join(",", row));
      sb.append(System.lineSeparator());
    }
    var is = new ByteArrayInputStream(sb.toString().getBytes(UTF_8));
    var part = mock(Part.class);
    when(part.getInputStream()).thenReturn(is);
    when(request.getPart("matrix")).thenReturn(part);
    var actualMatrix = extractMatrix(request, response);
    actualMatrix.ifPresent(am -> assertArrayEquals(convert(expectedMatrix), am));
  }

  @Test
  void testExtractRhs() throws ServletException, IOException {
    var expectedRhs = new String[] {"1.3", "2.2", "3.7"};
    var is =
        new ByteArrayInputStream(String.join(System.lineSeparator(), expectedRhs).getBytes(UTF_8));
    var part = mock(Part.class);
    when(part.getInputStream()).thenReturn(is);
    when(request.getPart("rhs")).thenReturn(part);
    var actualRhs = extractRhs(request, response);
    actualRhs.ifPresent(ar -> assertArrayEquals(convert(expectedRhs), ar));
  }
}

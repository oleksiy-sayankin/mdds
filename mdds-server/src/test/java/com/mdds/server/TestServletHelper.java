/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.github.valfirst.slf4jtest.TestLoggerFactory.getTestLogger;
import static com.mdds.server.ServletHelper.*;
import static jakarta.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.mdds.dto.SlaeSolver;
import com.mdds.queue.Queue;
import com.mdds.storage.DataStorage;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
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
    when(servletContext.getAttribute(ServerAppContextListener.ATTR_SERVER_SERVICE))
        .thenReturn(serverService);
    when(serverService.getQueue()).thenReturn(queue);
    var actualQueue = extractQueue(servletContext);
    actualQueue.ifPresent(q -> assertEquals(queue, q));
  }

  @Test
  void testExtractQueueWhenNoQueue() {
    var actualQueue = extractQueue(servletContext);
    assertTrue(actualQueue.isFailure());
    assertEquals(
        "Servlet context attribute SERVER_SERVICE is null.", actualQueue.getErrorMessage());
  }

  @Test
  void testExtractDataStorageWhenDataStorageExists() {
    when(servletContext.getAttribute(ServerAppContextListener.ATTR_SERVER_SERVICE))
        .thenReturn(serverService);
    when(serverService.getDataStorage()).thenReturn(dataStorage);
    var actualDataStorage = extractDataStorage(servletContext);
    actualDataStorage.ifPresent(ds -> assertEquals(dataStorage, ds));
  }

  @Test
  void testExtractSolvingMethodWhenMethodExists() {
    when(request.getParameter("slaeSolvingMethod")).thenReturn("numpy_exact_solver");
    var actualSolvingMethod = extractSolvingMethod(request);
    actualSolvingMethod.ifPresent(asm -> assertEquals(SlaeSolver.NUMPY_EXACT_SOLVER, asm));
  }

  @Test
  void testExtractSolvingMethodWhenWrongMethod() {
    var solvingMethod = "wrong_solving_method";
    when(request.getParameter("slaeSolvingMethod")).thenReturn(solvingMethod);
    var actualSolvingMethod = extractSolvingMethod(request);
    assertTrue(actualSolvingMethod.isFailure());
    assertEquals(
        "Invalid solving method: wrong_solving_method", actualSolvingMethod.getErrorMessage());
  }

  @Test
  void testExtractSolvingMethodWhenNullMethod() {
    when(request.getParameter("slaeSolvingMethod")).thenReturn(null);
    var actualSolvingMethod = extractSolvingMethod(request);
    assertTrue(actualSolvingMethod.isFailure());
    assertEquals("Invalid solving method: null", actualSolvingMethod.getErrorMessage());
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
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.mockito.Mockito.*;

import com.mdds.common.util.JsonHelper;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.TaskStatus;
import com.mdds.storage.DataStorage;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Enumeration;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestServerResultServlet {
  private ServerResultServlet servlet;
  private HttpServletRequest request;
  private HttpServletResponse response;
  private ServletContext servletContext;
  private PrintWriter printWriter;
  private DataStorage dataStorage;
  private ServerService serverService;

  @BeforeEach
  void setUp() throws ServletException {
    servlet = new ServerResultServlet();
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
    servletContext = mock(ServletContext.class);
    printWriter = mock(PrintWriter.class);
    dataStorage = mock(DataStorage.class);
    serverService = mock(ServerService.class);

    servlet.init(
        new ServletConfig() {
          @Override
          public String getServletName() {
            return "ServerResultServlet";
          }

          @Override
          public ServletContext getServletContext() {
            return servletContext;
          }

          @Override
          public String getInitParameter(String name) {
            return "empty";
          }

          @Override
          public Enumeration<String> getInitParameterNames() {
            return null;
          }
        });
  }

  @Test
  void testDoGetPathIsEmpty() throws IOException {
    when(servletContext.getAttribute(ServerAppContextListener.ATTR_SERVER_SERVICE))
        .thenReturn(serverService);
    servlet.doGet(request, response);
    verify(response).sendError(HttpServletResponse.SC_BAD_REQUEST, "Path is empty");
  }

  @Test
  void testDoGetNoTaskId() throws IOException {
    when(request.getPathInfo()).thenReturn("/result/");
    when(servletContext.getAttribute(ServerAppContextListener.ATTR_SERVER_SERVICE))
        .thenReturn(serverService);
    servlet.doGet(request, response);
    verify(response).sendError(HttpServletResponse.SC_BAD_REQUEST, "Task id missing");
  }

  @Test
  void testDoGetNoDataStorageInContext() throws IOException {
    when(request.getPathInfo()).thenReturn("/result/test_task_id");
    servlet.doGet(request, response);
    verify(response)
        .sendError(
            HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
            "Servlet context attribute SERVER_SERVICE is null.");
  }

  @Test
  void testDoGetNoResultForProvidedTaskId() throws IOException {
    when(request.getPathInfo()).thenReturn("/result/test_task_id");
    when(servletContext.getAttribute(ServerAppContextListener.ATTR_SERVER_SERVICE))
        .thenReturn(serverService);
    when(serverService.getDataStorage()).thenReturn(dataStorage);
    when(response.getWriter()).thenReturn(printWriter);
    servlet.doGet(request, response);
    verify(printWriter).write("{\"error\":\"no_result_for_provided_task_id\"}");
  }

  @Test
  void testDoGetDataIsInDataStorage() throws IOException {
    var taskId = "test_task_id";
    when(request.getPathInfo()).thenReturn("/result/" + taskId);
    when(servletContext.getAttribute(ServerAppContextListener.ATTR_SERVER_SERVICE))
        .thenReturn(serverService);
    when(serverService.getDataStorage()).thenReturn(dataStorage);
    when(response.getWriter()).thenReturn(printWriter);

    var expectedResult = new ResultDTO();
    expectedResult.setTaskId(taskId);
    expectedResult.setDateTimeTaskCreated(Instant.now());
    expectedResult.setDateTimeTaskFinished(Instant.now());
    expectedResult.setTaskStatus(TaskStatus.DONE);
    expectedResult.setProgress(100);
    expectedResult.setSolution(new double[] {81.1, 82.2, 37.3, 45.497});
    when(dataStorage.get(taskId, ResultDTO.class)).thenReturn(Optional.of(expectedResult));
    servlet.doGet(request, response);
    verify(printWriter).write(JsonHelper.toJson(expectedResult));
  }
}

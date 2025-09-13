/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.mockito.Mockito.*;

import com.mdds.storage.DataStorage;
import com.mdds.util.JsonHelper;
import dto.ResultDTO;
import dto.TaskStatus;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestResultServlet {
  private ResultServlet servlet;
  private HttpServletRequest request;
  private HttpServletResponse response;
  private ServletContext servletContext;
  private PrintWriter printWriter;
  private DataStorage dataStorage;

  @BeforeEach
  void setUp() {
    servlet = new ResultServlet();
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
    servletContext = mock(ServletContext.class);
    printWriter = mock(PrintWriter.class);
    dataStorage = mock(DataStorage.class);
  }

  @Test
  void testDoGetPathIsEmpty() throws IOException {
    servlet.doGet(request, response);
    verify(response).setContentType("application/json");
    verify(response).sendError(HttpServletResponse.SC_BAD_REQUEST, "Path is empty");
  }

  @Test
  void testDoGetNoTaskId() throws IOException {
    when(request.getPathInfo()).thenReturn("/result/");
    servlet.doGet(request, response);
    verify(response).sendError(HttpServletResponse.SC_BAD_REQUEST, "Task id missing");
  }

  @Test
  void testDoGetNoDataStorageInContext() throws IOException {
    when(request.getPathInfo()).thenReturn("/result/test_task_id");
    when(request.getServletContext()).thenReturn(servletContext);
    servlet.doGet(request, response);
    verify(response)
        .sendError(
            HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
            "Data Storage Service is not initialized");
  }

  @Test
  void testDoGetNoResultForProvidedTaskId() throws IOException {
    when(request.getPathInfo()).thenReturn("/result/test_task_id");
    when(request.getServletContext()).thenReturn(servletContext);
    when(servletContext.getAttribute(AppContextListener.ATTR_DATA_STORAGE)).thenReturn(dataStorage);
    when(response.getWriter()).thenReturn(printWriter);
    servlet.doGet(request, response);
    verify(printWriter).write("{\"error\":\"no_result_for_provided_task_id\"}");
  }

  @Test
  void testDoGetDataIsInDataStorage() throws IOException {
    var taskId = "test_task_id";
    when(request.getPathInfo()).thenReturn("/result/" + taskId);
    when(request.getServletContext()).thenReturn(servletContext);
    when(servletContext.getAttribute(AppContextListener.ATTR_DATA_STORAGE)).thenReturn(dataStorage);
    when(response.getWriter()).thenReturn(printWriter);

    var expectedResult = new ResultDTO();
    expectedResult.setTaskId(taskId);
    expectedResult.setDateTimeTaskCreated(Instant.now());
    expectedResult.setDateTimeTaskFinished(Instant.now());
    expectedResult.setTaskStatus(TaskStatus.DONE);
    expectedResult.setSolution(new double[] {81.1, 82.2, 37.3, 45.497});
    when(dataStorage.get(taskId, ResultDTO.class)).thenReturn(expectedResult);
    servlet.doGet(request, response);
    verify(printWriter).write(JsonHelper.toJson(expectedResult));
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static jakarta.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static jakarta.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.*;

import com.mdds.queue.Queue;
import com.mdds.storage.DataStorage;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.Part;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestSolveServlet {
  private SolveServlet servlet;
  private HttpServletRequest request;
  private HttpServletResponse response;
  private ServletContext servletContext;
  private Queue queue;
  private DataStorage dataStorage;

  @BeforeEach
  void setUp() {
    servlet = new SolveServlet();
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
    servletContext = mock(ServletContext.class);
    queue = mock(Queue.class);
    dataStorage = mock(DataStorage.class);
  }

  @Test
  void testDoPost() throws ServletException, IOException {
    // Prepare matrix of SLAE
    var expectedMatrix =
        new String[][] {{"1.3", "2.2", "3.7"}, {"7.7", "2.1", "9.3"}, {"1.1", "4.8", "2.3"}};
    var sb = new StringBuilder();
    for (var row : expectedMatrix) {
      sb.append(String.join(",", row));
      sb.append(System.lineSeparator());
    }
    var isMatrix = new ByteArrayInputStream(sb.toString().getBytes(UTF_8));
    var partMatrix = mock(Part.class);
    when(partMatrix.getInputStream()).thenReturn(isMatrix);
    when(request.getPart("matrix")).thenReturn(partMatrix);

    // Prepare right hand side of SLAE
    var expectedRhs = new String[] {"1.3", "2.2", "3.7"};
    var isRhs =
        new ByteArrayInputStream(String.join(System.lineSeparator(), expectedRhs).getBytes(UTF_8));
    var partRhs = mock(Part.class);
    when(partRhs.getInputStream()).thenReturn(isRhs);
    when(request.getPart("rhs")).thenReturn(partRhs);

    // Prepare solving method
    when(request.getParameter("slaeSolvingMethod")).thenReturn("numpy_exact_solver");

    // Prepare queue
    when(request.getServletContext()).thenReturn(servletContext);
    when(servletContext.getAttribute(AppContextListener.ATTR_TASK_QUEUE)).thenReturn(queue);

    // Prepare data storage
    when(request.getServletContext()).thenReturn(servletContext);
    when(servletContext.getAttribute(AppContextListener.ATTR_DATA_STORAGE)).thenReturn(dataStorage);
    var printWriter = mock(PrintWriter.class);
    when(response.getWriter()).thenReturn(printWriter);
    doNothing().when(printWriter).write(anyString());
    servlet.doPost(request, response);
    verify(response).setContentType("application/json");
    verify(printWriter).write(contains("id"));
  }

  @Test
  void testDoPostNoMatrix() throws ServletException, IOException {
    // Prepare right hand side of SLAE
    var expectedRhs = new String[] {"1.3", "2.2", "3.7"};
    var isRhs =
        new ByteArrayInputStream(String.join(System.lineSeparator(), expectedRhs).getBytes(UTF_8));
    var partRhs = mock(Part.class);
    when(partRhs.getInputStream()).thenReturn(isRhs);
    when(request.getPart("rhs")).thenReturn(partRhs);

    // Prepare solving method
    when(request.getParameter("slaeSolvingMethod")).thenReturn("numpy_exact_solver");

    // Prepare queue
    when(request.getServletContext()).thenReturn(servletContext);
    when(servletContext.getAttribute(AppContextListener.ATTR_TASK_QUEUE)).thenReturn(queue);

    // Prepare data storage
    when(request.getServletContext()).thenReturn(servletContext);
    when(servletContext.getAttribute(AppContextListener.ATTR_DATA_STORAGE)).thenReturn(dataStorage);
    var printWriter = mock(PrintWriter.class);
    when(response.getWriter()).thenReturn(printWriter);
    doNothing().when(printWriter).write(anyString());
    servlet.doPost(request, response);
    verify(response).sendError(SC_BAD_REQUEST, "Missing matrix in request");
  }

  @Test
  void testDoPostNoRhs() throws ServletException, IOException {
    // Prepare matrix of SLAE
    var expectedMatrix =
        new String[][] {{"1.3", "2.2", "3.7"}, {"7.7", "2.1", "9.3"}, {"1.1", "4.8", "2.3"}};
    var sb = new StringBuilder();
    for (var row : expectedMatrix) {
      sb.append(String.join(",", row));
      sb.append(System.lineSeparator());
    }
    var isMatrix = new ByteArrayInputStream(sb.toString().getBytes(UTF_8));
    var partMatrix = mock(Part.class);
    when(partMatrix.getInputStream()).thenReturn(isMatrix);
    when(request.getPart("matrix")).thenReturn(partMatrix);

    // Prepare solving method
    when(request.getParameter("slaeSolvingMethod")).thenReturn("numpy_exact_solver");

    // Prepare queue
    when(request.getServletContext()).thenReturn(servletContext);
    when(servletContext.getAttribute(AppContextListener.ATTR_TASK_QUEUE)).thenReturn(queue);

    // Prepare data storage
    when(request.getServletContext()).thenReturn(servletContext);
    when(servletContext.getAttribute(AppContextListener.ATTR_DATA_STORAGE)).thenReturn(dataStorage);
    var printWriter = mock(PrintWriter.class);
    when(response.getWriter()).thenReturn(printWriter);
    doNothing().when(printWriter).write(anyString());
    servlet.doPost(request, response);
    verify(response).sendError(SC_BAD_REQUEST, "Missing right hand side in request");
  }

  @Test
  void testDoPostNoSolvingMethod() throws ServletException, IOException {
    // Prepare matrix of SLAE
    var expectedMatrix =
        new String[][] {{"1.3", "2.2", "3.7"}, {"7.7", "2.1", "9.3"}, {"1.1", "4.8", "2.3"}};
    var sb = new StringBuilder();
    for (var row : expectedMatrix) {
      sb.append(String.join(",", row));
      sb.append(System.lineSeparator());
    }
    var isMatrix = new ByteArrayInputStream(sb.toString().getBytes(UTF_8));
    var partMatrix = mock(Part.class);
    when(partMatrix.getInputStream()).thenReturn(isMatrix);
    when(request.getPart("matrix")).thenReturn(partMatrix);

    // Prepare right hand side of SLAE
    var expectedRhs = new String[] {"1.3", "2.2", "3.7"};
    var isRhs =
        new ByteArrayInputStream(String.join(System.lineSeparator(), expectedRhs).getBytes(UTF_8));
    var partRhs = mock(Part.class);
    when(partRhs.getInputStream()).thenReturn(isRhs);
    when(request.getPart("rhs")).thenReturn(partRhs);

    // Prepare queue
    when(request.getServletContext()).thenReturn(servletContext);
    when(servletContext.getAttribute(AppContextListener.ATTR_TASK_QUEUE)).thenReturn(queue);

    // Prepare data storage
    when(request.getServletContext()).thenReturn(servletContext);
    when(servletContext.getAttribute(AppContextListener.ATTR_DATA_STORAGE)).thenReturn(dataStorage);
    var printWriter = mock(PrintWriter.class);
    when(response.getWriter()).thenReturn(printWriter);
    doNothing().when(printWriter).write(anyString());
    servlet.doPost(request, response);
    verify(response).sendError(SC_BAD_REQUEST, "Invalid solving method: null");
  }

  @Test
  void testDoPostNoQueue() throws ServletException, IOException {
    // Prepare matrix of SLAE
    var expectedMatrix =
        new String[][] {{"1.3", "2.2", "3.7"}, {"7.7", "2.1", "9.3"}, {"1.1", "4.8", "2.3"}};
    var sb = new StringBuilder();
    for (var row : expectedMatrix) {
      sb.append(String.join(",", row));
      sb.append(System.lineSeparator());
    }
    var isMatrix = new ByteArrayInputStream(sb.toString().getBytes(UTF_8));
    var partMatrix = mock(Part.class);
    when(partMatrix.getInputStream()).thenReturn(isMatrix);
    when(request.getPart("matrix")).thenReturn(partMatrix);

    // Prepare right hand side of SLAE
    var expectedRhs = new String[] {"1.3", "2.2", "3.7"};
    var isRhs =
        new ByteArrayInputStream(String.join(System.lineSeparator(), expectedRhs).getBytes(UTF_8));
    var partRhs = mock(Part.class);
    when(partRhs.getInputStream()).thenReturn(isRhs);
    when(request.getPart("rhs")).thenReturn(partRhs);

    // Prepare solving method
    when(request.getParameter("slaeSolvingMethod")).thenReturn("numpy_exact_solver");

    // Prepare data storage
    when(request.getServletContext()).thenReturn(servletContext);
    when(servletContext.getAttribute(AppContextListener.ATTR_DATA_STORAGE)).thenReturn(dataStorage);
    var printWriter = mock(PrintWriter.class);
    when(response.getWriter()).thenReturn(printWriter);
    doNothing().when(printWriter).write(anyString());
    servlet.doPost(request, response);
    verify(response).sendError(SC_INTERNAL_SERVER_ERROR, "Service Queue is not initialized");
  }

  @Test
  void testDoPostNoDataStorage() throws ServletException, IOException {
    // Prepare matrix of SLAE
    var expectedMatrix =
        new String[][] {{"1.3", "2.2", "3.7"}, {"7.7", "2.1", "9.3"}, {"1.1", "4.8", "2.3"}};
    var sb = new StringBuilder();
    for (var row : expectedMatrix) {
      sb.append(String.join(",", row));
      sb.append(System.lineSeparator());
    }
    var isMatrix = new ByteArrayInputStream(sb.toString().getBytes(UTF_8));
    var partMatrix = mock(Part.class);
    when(partMatrix.getInputStream()).thenReturn(isMatrix);
    when(request.getPart("matrix")).thenReturn(partMatrix);

    // Prepare right hand side of SLAE
    var expectedRhs = new String[] {"1.3", "2.2", "3.7"};
    var isRhs =
        new ByteArrayInputStream(String.join(System.lineSeparator(), expectedRhs).getBytes(UTF_8));
    var partRhs = mock(Part.class);
    when(partRhs.getInputStream()).thenReturn(isRhs);
    when(request.getPart("rhs")).thenReturn(partRhs);

    // Prepare solving method
    when(request.getParameter("slaeSolvingMethod")).thenReturn("numpy_exact_solver");

    // Prepare queue
    when(request.getServletContext()).thenReturn(servletContext);
    when(servletContext.getAttribute(AppContextListener.ATTR_TASK_QUEUE)).thenReturn(queue);

    var printWriter = mock(PrintWriter.class);
    when(response.getWriter()).thenReturn(printWriter);
    doNothing().when(printWriter).write(anyString());
    servlet.doPost(request, response);
    verify(response).sendError(SC_INTERNAL_SERVER_ERROR, "Data Storage Service is not initialized");
  }
}

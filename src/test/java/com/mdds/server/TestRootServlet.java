/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.github.valfirst.slf4jtest.TestLoggerFactory.getTestLogger;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import com.github.valfirst.slf4jtest.TestLogger;
import com.github.valfirst.slf4jtest.TestLoggerFactoryExtension;
import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestLoggerFactoryExtension.class)
class TestRootServlet {
  private RootServlet servlet;
  private HttpServletRequest request;
  private HttpServletResponse response;
  private RequestDispatcher dispatcher;

  @BeforeEach
  void setUp() {
    servlet = new RootServlet();
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
    dispatcher = mock(RequestDispatcher.class);
  }

  @Test
  void testDoGetForwardsToIndexHtml() throws Exception {
    when(request.getRequestDispatcher("/index.html")).thenReturn(dispatcher);
    servlet.doGet(request, response);
    verify(dispatcher).forward(request, response);
    verify(response).setContentType("text/html");
  }

  @Test
  void testDoGetWhenIndexHtmlNotFound() throws Exception {
    when(request.getRequestDispatcher("/index.html")).thenReturn(null);
    servlet.doGet(request, response);
    verify(response).sendError(HttpServletResponse.SC_NOT_FOUND, "index.html not found");
  }

  @Test
  void testDoGetHandlesException() throws Exception {
    TestLogger logger = getTestLogger(RootServlet.class);
    when(request.getRequestDispatcher("/index.html")).thenReturn(dispatcher);
    doThrow(new ServletException("Simulated error")).when(dispatcher).forward(request, response);
    servlet.doGet(request, response);
    verify(response)
        .sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Internal server error");
    assertTrue(
        logger.getLoggingEvents().stream()
            .anyMatch(e -> e.getMessage().contains("Error processing root servlet")));
  }
}

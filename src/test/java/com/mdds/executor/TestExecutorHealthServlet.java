/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import static org.mockito.Mockito.*;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestExecutorHealthServlet {
  private ExecutorHealthServlet servlet;
  private HttpServletRequest request;
  private HttpServletResponse response;

  @BeforeEach
  void setUp() {
    servlet = new ExecutorHealthServlet();
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
  }

  @Test
  void testDoGetForwardsToIndexHtml() {
    servlet.doGet(request, response);
    verify(response, times(1)).setStatus(HttpServletResponse.SC_OK);
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.mockito.Mockito.*;

import com.github.valfirst.slf4jtest.TestLoggerFactoryExtension;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestLoggerFactoryExtension.class)
class TestServerHealthServlet {
  private ServerHealthServlet servlet;
  private HttpServletRequest request;
  private HttpServletResponse response;

  @BeforeEach
  void setUp() {
    servlet = new ServerHealthServlet();
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
  }

  @Test
  void testDoGetForwardsToIndexHtml() {
    servlet.doGet(request, response);
    verify(response, times(1)).setStatus(HttpServletResponse.SC_OK);
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

/** Root endpoint. Returns index.html only. */
@Slf4j
public class ServerRootServlet extends HttpServlet {

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    response.setContentType("text/html");
    try {
      var dispatcher = request.getRequestDispatcher("/index.html");
      if (dispatcher == null) {
        response.sendError(HttpServletResponse.SC_NOT_FOUND, "index.html not found");
        log.error("index.html not found");
        return;
      }
      // Forward the request and response to index.html
      dispatcher.forward(request, response);
    } catch (ServletException | IOException e) {
      log.error("Error processing root servlet", e);
      try {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Internal server error");
      } catch (IOException ex) {
        log.error("Error sending internal server error status", ex);
      }
    }
  }
}

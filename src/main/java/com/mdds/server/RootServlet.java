/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Root endpoint. Returns index.html only. */
@WebServlet(
    name = "rootServlet",
    urlPatterns = {"/"})
public class RootServlet extends HttpServlet {
  private static final Logger LOGGER = LoggerFactory.getLogger(RootServlet.class);

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    response.setContentType("text/html");
    try {
      var dispatcher = request.getRequestDispatcher("/index.html");
      if (dispatcher == null) {
        response.sendError(HttpServletResponse.SC_NOT_FOUND, "index.html not found");
        LOGGER.error("index.html not found");
        return;
      }
      // Forward the request and response to index.html
      dispatcher.forward(request, response);
    } catch (ServletException | IOException e) {
      LOGGER.error("Error processing root servlet", e);
      try {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Internal server error");
      } catch (IOException ex) {
        LOGGER.error("Error sending internal server error status", ex);
      }
    }
  }
}

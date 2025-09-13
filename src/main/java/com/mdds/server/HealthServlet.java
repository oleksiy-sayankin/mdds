/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Health endpoint. Returns status ok. */
@WebServlet(
    name = "healthServlet",
    urlPatterns = {"/health"})
public class HealthServlet extends HttpServlet {
  private static final Logger LOGGER = LoggerFactory.getLogger(HealthServlet.class);

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    response.setStatus(HttpServletResponse.SC_OK);
    LOGGER.info("Health check is ok");
  }
}

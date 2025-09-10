/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.server.Main.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TestMain {
  private static Tomcat tomcat;
  private static final String MDDS_SERVER_HOST =
      System.getenv().getOrDefault("MDDS_SERVER_HOST", MDDS_SERVER_DEFAULT_HOST);
  private static int mddsServerPort =
      Integer.parseInt(
          System.getenv()
              .getOrDefault("MDDS_SERVER_PORT", String.valueOf(MDDS_SERVER_DEFAULT_PORT)));
  private static final String MDDS_SERVER_WEB_APPLICATION_LOCATION =
      System.getenv()
          .getOrDefault(
              "MDDS_SERVER_WEB_APPLICATION_LOCATION", MDDS_SERVER_DEFAULT_WEB_APPLICATION_LOCATION);

  @BeforeAll
  static void startServer() throws LifecycleException {
    tomcat = Main.start(MDDS_SERVER_HOST, 0, MDDS_SERVER_WEB_APPLICATION_LOCATION);
    mddsServerPort = tomcat.getConnector().getLocalPort();
  }

  @AfterAll
  static void stopServer() throws LifecycleException {
    if (tomcat != null) {
      tomcat.stop();
      tomcat.destroy();
    }
  }

  @Test
  void testRootReturnsIndexHtml() throws Exception {
    URI uri = new URI("http://" + MDDS_SERVER_HOST + ":" + String.valueOf(mddsServerPort));
    URL url = uri.toURL();
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");

    assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    assertEquals("text/html", connection.getContentType());

    try (var reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
      String body = reader.lines().reduce("", (a, b) -> a + b);
      assertTrue(body.contains("<html"));
    }
  }
}

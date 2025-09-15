/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.server.Main.*;
import static com.mdds.util.CustomHelper.findFreePort;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mdds.storage.DataStorageFactory;
import com.mdds.storage.redis.RedisHelper;
import com.mdds.util.JsonHelper;
import dto.ResultDTO;
import dto.TaskStatus;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.embedded.RedisServer;

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
              "MDDS_SERVER_WEB_APPLICATION_LOCATION",
              new File(MDDS_SERVER_DEFAULT_WEB_APPLICATION_LOCATION).getAbsolutePath());
  private static final int REDIS_SERVER_PORT = findFreePort();
  private static RedisServer redisServer;

  @BeforeAll
  static void startServer() throws LifecycleException, IOException {
    redisServer = new RedisServer(REDIS_SERVER_PORT);
    redisServer.start();
    tomcat = Main.start(MDDS_SERVER_HOST, 0, MDDS_SERVER_WEB_APPLICATION_LOCATION);
    mddsServerPort = tomcat.getConnector().getLocalPort();
  }

  @AfterAll
  static void stopServer() throws LifecycleException, IOException {
    if (tomcat != null) {
      tomcat.stop();
      tomcat.destroy();
    }
    if (redisServer != null) {
      redisServer.stop();
    }
  }

  @Test
  void testRootReturnsIndexHtml() throws Exception {
    var uri = new URI("http://" + MDDS_SERVER_HOST + ":" + String.valueOf(mddsServerPort));
    var url = uri.toURL();
    var connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");

    assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    assertEquals("text/html", connection.getContentType());

    try (var reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
      String body = reader.lines().reduce("", (a, b) -> a + b);
      assertTrue(body.contains("<html"));
    }
  }

  @Test
  void testHealthReturnsStatusOk() throws URISyntaxException, IOException {
    var uri = new URI("http://" + MDDS_SERVER_HOST + ":" + mddsServerPort + "/health");
    var url = uri.toURL();
    var connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
  }

  @Test
  void testResultReturnsDataFromDataStorage() throws URISyntaxException, IOException {
    var taskId = "test_task_id";
    // Create Result and put it to storage manually
    var expectedResult = new ResultDTO();
    expectedResult.setTaskId(taskId);
    expectedResult.setDateTimeTaskCreated(Instant.now());
    expectedResult.setDateTimeTaskFinished(Instant.now());
    expectedResult.setTaskStatus(TaskStatus.DONE);
    expectedResult.setSolution(new double[] {81.1, 82.2, 37.3, 45.497});

    // We expect Redis service is up and running here
    try (var storage =
        DataStorageFactory.createRedis(RedisHelper.readFromResources("redis.properties"))) {
      storage.put(taskId, expectedResult);
      // Test that data is in data storage
      var actualResult = storage.get(taskId, ResultDTO.class);
      Assertions.assertEquals(
          expectedResult, actualResult.isPresent() ? actualResult.get() : actualResult);
    }

    // Request result using endpoint
    var uri = new URI("http://" + MDDS_SERVER_HOST + ":" + mddsServerPort + "/result/" + taskId);
    var url = uri.toURL();
    var connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    assertEquals("application/json;charset=ISO-8859-1", connection.getContentType());

    // Read result from the response.
    var sb = new StringBuilder();
    try (var reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
    }
    var body = sb.toString();
    var actualResult = JsonHelper.fromJson(body, ResultDTO.class);
    assertEquals(expectedResult, actualResult);
  }
}

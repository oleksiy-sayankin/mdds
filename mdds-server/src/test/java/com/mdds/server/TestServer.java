/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.common.util.CommonHelper.findFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import com.mdds.common.util.JsonHelper;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.TaskStatus;
import com.mdds.storage.DataStorageFactory;
import com.mdds.storage.redis.RedisConfFactory;
import com.rabbitmq.client.ConnectionFactory;
import java.io.*;
import java.net.*;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;
import redis.embedded.RedisServer;

@Testcontainers
class TestServer {
  private static Tomcat tomcat;
  private static final String MDDS_SERVER_HOST =
      ServerConfFactory.fromEnvOrDefaultProperties().host();
  private static final int MDDS_SERVER_PORT = findFreePort();
  private static final String MDDS_SERVER_WEB_APPLICATION_LOCATION =
      ServerConfFactory.fromEnvOrDefaultProperties().webappDirLocation();
  private static final int REDIS_SERVER_PORT = findFreePort();
  private static RedisServer redisServer;

  @Container
  private static final RabbitMQContainer rabbitMq =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672);

  @BeforeAll
  static void startServer() throws LifecycleException, IOException {
    redisServer = new RedisServer(REDIS_SERVER_PORT);
    redisServer.start();

    // Wait for RabbitMq is ready
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(TestServer::queueIsReady);

    System.setProperty("redis.host", "localhost");
    System.setProperty("redis.port", String.valueOf(REDIS_SERVER_PORT));
    System.setProperty("rabbitmq.host", rabbitMq.getHost());
    System.setProperty("rabbitmq.port", String.valueOf(rabbitMq.getAmqpPort()));
    System.setProperty("rabbitmq.user", rabbitMq.getAdminUsername());
    System.setProperty("rabbitmq.password", rabbitMq.getAdminPassword());

    tomcat = Server.start(MDDS_SERVER_HOST, MDDS_SERVER_PORT, MDDS_SERVER_WEB_APPLICATION_LOCATION);
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
    var uri = new URI("http://" + MDDS_SERVER_HOST + ":" + MDDS_SERVER_PORT);
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
    var uri = new URI("http://" + MDDS_SERVER_HOST + ":" + MDDS_SERVER_PORT + "/health");
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
        DataStorageFactory.createRedis(RedisConfFactory.fromEnvOrDefaultProperties())) {
      storage.put(taskId, expectedResult);
      // Test that data is in data storage
      var actualResult = storage.get(taskId, ResultDTO.class);
      Assertions.assertEquals(
          expectedResult, actualResult.isPresent() ? actualResult.get() : actualResult);
    }

    // Request result using endpoint
    var uri = new URI("http://" + MDDS_SERVER_HOST + ":" + MDDS_SERVER_PORT + "/result/" + taskId);
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

  @Test
  void testSolve() throws Exception {
    var uri = new URI("http://" + MDDS_SERVER_HOST + ":" + MDDS_SERVER_PORT + "/solve");
    var url = uri.toURL();

    String boundary = "----TestBoundary";
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setDoOutput(true);
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

    try (var output = connection.getOutputStream()) {
      var writer = new PrintWriter(new OutputStreamWriter(output, UTF_8), true);

      // add slaeSolvingMethod
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"slaeSolvingMethod\"\r\n\r\n");
      writer.append("numpy_exact_solver").append("\r\n");
      writer.flush();

      // add matrix
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"matrix\"; filename=\"matrix.csv\"\r\n");
      writer.append("Content-Type: text/csv\r\n\r\n");
      writer.flush();
      output.write("1.3,2.4,3.1\n4.77,5.2321,6.32\n7.23,8.43,9.4343\n".getBytes(UTF_8));
      output.write("\r\n".getBytes(UTF_8));
      output.flush();

      // put rhs
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"rhs\"; filename=\"rhs.csv\"\r\n");
      writer.append("Content-Type: text/csv\r\n\r\n");
      writer.flush();
      output.write("1.3\n2.2\n3.7\n".getBytes(UTF_8));
      output.write("\r\n".getBytes(UTF_8));
      output.flush();

      // finish the request
      writer.append("--").append(boundary).append("--").append("\r\n");
      writer.close();
    }

    // Check the answer
    assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    assertEquals("application/json;charset=ISO-8859-1", connection.getContentType());

    try (var reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
      var responseBody = reader.lines().reduce("", (a, b) -> a + b);
      assertTrue(responseBody.contains("id"));
    }
  }

  private static boolean queueIsReady() throws IOException, TimeoutException {
    try (var connection =
        createConnectionFactory(
                rabbitMq.getHost(),
                rabbitMq.getAmqpPort(),
                rabbitMq.getAdminUsername(),
                rabbitMq.getAdminPassword())
            .newConnection()) {
      return connection.isOpen();
    }
  }

  private static ConnectionFactory createConnectionFactory(
      String host, int port, String user, String password) {
    var factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername(user);
    factory.setPassword(password);
    return factory;
  }
}

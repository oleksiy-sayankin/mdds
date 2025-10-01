/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import static com.mdds.util.CustomHelper.findFreePort;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mdds.queue.*;
import com.mdds.queue.rabbitmq.RabbitMqQueueProvider;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
class TestExecutor {
  private static Tomcat tomcat;
  private static final String MDDS_EXECUTOR_HOST =
      ExecutorConfFactory.fromEnvOrDefaultProperties().host();
  private static final int MDDS_EXECUTOR_PORT = findFreePort();
  private static final String MDDS_EXECUTOR_APPLICATION_LOCATION =
      ExecutorConfFactory.fromEnvOrDefaultProperties().webappDirLocation();
  private static Queue resultQueue;
  private static Queue taskQueue;

  @Container
  private static final RabbitMQContainer rabbitMq =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672);

  @BeforeAll
  static void startServer() throws LifecycleException {
    System.setProperty("rabbitmq.host", rabbitMq.getHost());
    System.setProperty("rabbitmq.port", String.valueOf(rabbitMq.getAmqpPort()));
    System.setProperty("rabbitmq.user", rabbitMq.getAdminUsername());
    System.setProperty("rabbitmq.password", rabbitMq.getAdminPassword());
    resultQueue = new RabbitMqQueueProvider().get();
    taskQueue = new RabbitMqQueueProvider().get();
    tomcat =
        Executor.start(MDDS_EXECUTOR_HOST, MDDS_EXECUTOR_PORT, MDDS_EXECUTOR_APPLICATION_LOCATION);
  }

  @AfterAll
  static void stopServer() throws LifecycleException {
    if (tomcat != null) {
      tomcat.stop();
      tomcat.destroy();
    }
    if (taskQueue != null) {
      taskQueue.close();
    }
    if (resultQueue != null) {
      resultQueue.close();
    }
  }

  @Test
  void testHealthReturnsStatusOk() throws URISyntaxException, IOException {
    var uri = new URI("http://" + MDDS_EXECUTOR_HOST + ":" + MDDS_EXECUTOR_PORT + "/health");
    var url = uri.toURL();
    var connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
  }
}

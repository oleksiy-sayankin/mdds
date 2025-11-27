/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.result;

import static com.mdds.common.util.CommonHelper.findFreePort;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mdds.common.AppConstants;
import com.mdds.common.AppConstantsFactory;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.TaskStatus;
import com.mdds.queue.Message;
import com.mdds.queue.QueueFactory;
import com.mdds.storage.DataStorageFactory;
import io.restassured.RestAssured;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;
import redis.embedded.RedisServer;

@Testcontainers
class TestResultConsumer {
  private static Tomcat tomcat;
  private static final String HOST = ResultConsumerConfFactory.fromEnvOrDefaultProperties().host();
  private static final int PORT = findFreePort();
  private static final String WEBAPP =
      ResultConsumerConfFactory.fromEnvOrDefaultProperties().webappDirLocation();
  private static final int REDIS_PORT = findFreePort();
  private static RedisServer redisServer;

  @Container
  private static final RabbitMQContainer rabbitMq =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672);

  @BeforeAll
  static void startServer() throws LifecycleException, IOException {
    redisServer = new RedisServer(REDIS_PORT);
    redisServer.start();
    System.setProperty("redis.host", "localhost");
    System.setProperty("redis.port", String.valueOf(REDIS_PORT));
    System.setProperty("rabbitmq.host", rabbitMq.getHost());
    System.setProperty("rabbitmq.port", String.valueOf(rabbitMq.getAmqpPort()));
    System.setProperty("rabbitmq.user", rabbitMq.getAdminUsername());
    System.setProperty("rabbitmq.password", rabbitMq.getAdminPassword());

    tomcat = ResultConsumer.start(HOST, PORT, WEBAPP);
    RestAssured.baseURI = "http://" + HOST + ":" + PORT;
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
  void testHealthReturnsStatusOk() {
    given().when().get("/health").then().statusCode(200);
  }

  @Test
  void testResultConsumerTakesResultFromQueue() {
    // Prepare and put data to result queue
    var expected = new ResultDTO();
    var taskId = "test";
    expected.setTaskId(taskId);
    expected.setDateTimeTaskCreated(Instant.now());
    expected.setDateTimeTaskFinished(Instant.now());
    expected.setTaskStatus(TaskStatus.DONE);
    expected.setProgress(100);
    expected.setSolution(new double[] {1.1, 2.2, 3.3, 4.4});
    var message = new Message<>(expected, new HashMap<>(), Instant.now());
    try (var queue =
        QueueFactory.createRabbitMq(
            rabbitMq.getHost(),
            rabbitMq.getAmqpPort(),
            rabbitMq.getAdminUsername(),
            rabbitMq.getAdminPassword())) {
      queue.publish(AppConstantsFactory.getString(AppConstants.RESULT_QUEUE_NAME), message);
    }

    // Read result from Data Storage
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              try (var storage = DataStorageFactory.createRedis("localhost", REDIS_PORT)) {
                var actualResult = storage.get(taskId, ResultDTO.class);
                assertTrue(actualResult.isPresent());
                actualResult.ifPresent(ar -> assertEquals(expected, ar));
              }
            });
  }

  @Test
  void testResultConsumerTakesThreeResultItemsFromQueue() {
    // Prepare and put data to result queue
    var taskId1 = UUID.randomUUID().toString();
    var expectedResult1 =
        new ResultDTO(
            taskId1,
            Instant.now(),
            Instant.now(),
            TaskStatus.DONE,
            100,
            new double[] {1.1, 2.2, 3.3, 4.4},
            "");
    var message1 = new Message<>(expectedResult1, new HashMap<>(), Instant.now());

    var taskId2 = UUID.randomUUID().toString();
    var expectedResult2 =
        new ResultDTO(
            taskId2,
            Instant.now(),
            Instant.now(),
            TaskStatus.DONE,
            100,
            new double[] {2.1, 3.2, 3.3, 4.4},
            "");
    var message2 = new Message<>(expectedResult2, new HashMap<>(), Instant.now());

    var taskId3 = UUID.randomUUID().toString();
    var expectedResult3 =
        new ResultDTO(
            taskId3,
            Instant.now(),
            Instant.now(),
            TaskStatus.DONE,
            100,
            new double[] {3.1, 4.2, 3.3, 4.4},
            "");
    var message3 = new Message<>(expectedResult3, new HashMap<>(), Instant.now());

    try (var queue =
        QueueFactory.createRabbitMq(
            rabbitMq.getHost(),
            rabbitMq.getAmqpPort(),
            rabbitMq.getAdminUsername(),
            rabbitMq.getAdminPassword())) {
      queue.publish(AppConstantsFactory.getString(AppConstants.RESULT_QUEUE_NAME), message1);
      queue.publish(AppConstantsFactory.getString(AppConstants.RESULT_QUEUE_NAME), message2);
      queue.publish(AppConstantsFactory.getString(AppConstants.RESULT_QUEUE_NAME), message3);
    }

    // Read result from Data Storage
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              try (var storage = DataStorageFactory.createRedis("localhost", REDIS_PORT)) {
                var actualResult1 = storage.get(taskId1, ResultDTO.class);
                assertTrue(actualResult1.isPresent());
                actualResult1.ifPresent(ar -> assertEquals(expectedResult1, ar));

                var actualResult2 = storage.get(taskId2, ResultDTO.class);
                assertTrue(actualResult2.isPresent());
                actualResult2.ifPresent(ar -> assertEquals(expectedResult2, ar));

                var actualResult3 = storage.get(taskId3, ResultDTO.class);
                assertTrue(actualResult3.isPresent());
                actualResult3.ifPresent(ar -> assertEquals(expectedResult3, ar));
              }
            });
  }
}

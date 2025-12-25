/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.result;

import static com.mdds.common.util.CommonHelper.findFreePort;
import static org.assertj.core.api.Assertions.assertThat;

import com.mdds.common.CommonProperties;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.TaskStatus;
import com.mdds.queue.Message;
import com.mdds.queue.rabbitmq.RabbitMqQueue;
import com.mdds.storage.redis.RedisDataStorage;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import redis.embedded.RedisServer;

@Slf4j
@SpringBootTest(
    classes = ResultConsumerApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class TestResultConsumerApplication {
  @Autowired private ResultConsumerService resultConsumerApplication;
  @Autowired private CommonProperties commonProperties;

  private static final String HOST = "localhost";
  private static final int PORT = findFreePort();
  private static final int REDIS_PORT = findFreePort();
  private static final RedisServer redisServer;
  private static final RabbitMQContainer rabbitMq;

  static {
    rabbitMq =
        new RabbitMQContainer("rabbitmq:3.12-management")
            .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
            .withExposedPorts(5672, 15672)
            .waitingFor(Wait.forListeningPort())
            .withStartupTimeout(Duration.ofSeconds(30));
    rabbitMq.start();
    log.info("RabbitMq container is ready {}:{}", rabbitMq.getHost(), rabbitMq.getAmqpPort());
    try {
      redisServer = new RedisServer(REDIS_PORT);
      redisServer.start();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
    log.info("Embedded Redis Server is ready {}:{}", HOST, REDIS_PORT);
  }

  @DynamicPropertySource
  static void initProps(DynamicPropertyRegistry registry) {
    registry.add("mdds.result.consumer.host", () -> HOST);
    registry.add("mdds.result.consumer.port", () -> PORT);
    registry.add("mdds.rabbitmq.host", rabbitMq::getHost);
    registry.add("mdds.rabbitmq.port", rabbitMq::getAmqpPort);
    registry.add("mdds.rabbitmq.user", rabbitMq::getAdminUsername);
    registry.add("mdds.rabbitmq.password", rabbitMq::getAdminPassword);
    registry.add("mdds.redis.host", () -> HOST);
    registry.add("mdds.redis.port", () -> String.valueOf(REDIS_PORT));
  }

  @AfterAll
  static void stopServer() throws IOException {
    redisServer.stop();
    rabbitMq.stop();
  }

  @Test
  void testHealthReturnsStatusOk() throws IOException, InterruptedException {
    HttpResponse<Void> response;
    try (var client = HttpClient.newHttpClient()) {
      var request =
          HttpRequest.newBuilder()
              .uri(URI.create("http://" + HOST + ":" + PORT + "/health"))
              .GET()
              .build();
      response = client.send(request, java.net.http.HttpResponse.BodyHandlers.discarding());
    }
    assertThat(response.statusCode()).isEqualTo(HttpURLConnection.HTTP_OK);
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
        new RabbitMqQueue(
            rabbitMq.getHost(),
            rabbitMq.getAmqpPort(),
            rabbitMq.getAdminUsername(),
            rabbitMq.getAdminPassword())) {
      queue.publish(commonProperties.getResultQueueName(), message);
    }

    // Read result from Data Storage
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              try (var storage = new RedisDataStorage("localhost", REDIS_PORT)) {
                var actualResult = storage.get(taskId, ResultDTO.class);
                assertThat(actualResult).isPresent();
                actualResult.ifPresent(ar -> assertThat(ar).isEqualTo(expected));
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
            "cancel.queue-executor-0001",
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
            "cancel.queue-executor-0001",
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
            "cancel.queue-executor-0001",
            100,
            new double[] {3.1, 4.2, 3.3, 4.4},
            "");
    var message3 = new Message<>(expectedResult3, new HashMap<>(), Instant.now());

    try (var queue =
        new RabbitMqQueue(
            rabbitMq.getHost(),
            rabbitMq.getAmqpPort(),
            rabbitMq.getAdminUsername(),
            rabbitMq.getAdminPassword())) {
      queue.publish(commonProperties.getResultQueueName(), message1);
      queue.publish(commonProperties.getResultQueueName(), message2);
      queue.publish(commonProperties.getResultQueueName(), message3);
    }

    // Read result from Data Storage
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              try (var storage = new RedisDataStorage("localhost", REDIS_PORT)) {
                var actualResult1 = storage.get(taskId1, ResultDTO.class);
                assertThat(actualResult1).isPresent();
                actualResult1.ifPresent(ar -> assertThat(ar).isEqualTo(expectedResult1));

                var actualResult2 = storage.get(taskId2, ResultDTO.class);
                assertThat(actualResult2).isPresent();
                actualResult2.ifPresent(ar -> assertThat(ar).isEqualTo(expectedResult2));

                var actualResult3 = storage.get(taskId3, ResultDTO.class);
                assertThat(actualResult3).isPresent();
                actualResult3.ifPresent(ar -> assertThat(ar).isEqualTo(expectedResult3));
              }
            });
  }
}

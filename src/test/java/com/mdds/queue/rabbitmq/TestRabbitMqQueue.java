/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import static com.mdds.queue.rabbitmq.RabbitMqHelper.readFromResources;
import static dto.SlaeSolver.NUMPY_EXACT_SOLVER;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.mdds.queue.*;
import com.rabbitmq.client.Channel;
import dto.TaskDTO;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
class TestRabbitMqQueue {
  private static final String TASK_QUEUE_NAME = "task_queue";

  @Container
  private static final RabbitMQContainer rabbitMq =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672);

  private static String host;
  private static int port;
  private static String user;
  private static String password;

  @BeforeAll
  static void init() {
    host = rabbitMq.getHost();
    port = rabbitMq.getAmqpPort();
    user = rabbitMq.getAdminUsername();
    password = rabbitMq.getAdminPassword();
  }

  @Test
  void testNoConfFileExists() {
    var randomHost = "random.host";
    var randomPort = 89798;
    var randomUser = "random";
    var randomPassword = "random";
    assertThrows(
        RabbitMqConnectionException.class,
        () -> {
          try (var queue = new RabbitMqQueue(randomHost, randomPort, randomUser, randomPassword)) {
            // Do nothing.
          }
        });
  }

  @Test
  void testPublish() {
    var taskId = "test_id";
    var timeCreated = Instant.now();
    var expectedTask = new TaskDTO();
    expectedTask.setRhs(new double[] {73.4, 764.6});
    expectedTask.setMatrix(new double[][] {{783.7, 757.6}, {72.9, 4.75}});
    expectedTask.setId(taskId);
    expectedTask.setDateTime(timeCreated);
    expectedTask.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    Map<String, Object> headers = new HashMap<>();
    var message = new Message<>(expectedTask, headers, Instant.now());
    try (var queue = new RabbitMqQueue(host, port, user, password)) {
      queue.publish(TASK_QUEUE_NAME, message);
      Assertions.assertDoesNotThrow(() -> queue.publish(TASK_QUEUE_NAME, message));
    }
  }

  @Test
  void testPublishWithException() throws IOException {
    var mockChannel = mock(Channel.class);
    doThrow(new IOException("Simulated failure"))
        .when(mockChannel)
        .basicPublish(anyString(), anyString(), any(), any());
    var message = new Message<>("payload", Map.of(), Instant.now());
    try (var queue = new RabbitMqQueue(host, port, user, password)) {
      queue.setChannel(mockChannel);
      assertThrows(
          RabbitMqConnectionException.class, () -> queue.publish(TASK_QUEUE_NAME, message));
    }
  }

  @Test
  void testNoConnectionToRabbitMq() {
    var properties = readFromResources("no.connection.rabbitmq.properties");
    assertThrows(
        RabbitMqConnectionException.class,
        () -> {
          try (var queue = new RabbitMqQueue(properties)) {
            // Do nothing.
          }
        });
  }

  @Test
  void testDeleteQueue() {
    var taskId = "test_id";
    var timeCreated = Instant.now();
    var expectedTask = new TaskDTO();
    expectedTask.setRhs(new double[] {3.4, 4.6});
    expectedTask.setMatrix(new double[][] {{3.7, 5.6}, {2.9, 4.5}});
    expectedTask.setId(taskId);
    expectedTask.setDateTime(timeCreated);
    expectedTask.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    Map<String, Object> headers = new HashMap<>();
    var message = new Message<>(expectedTask, headers, Instant.now());
    try (var queue = new RabbitMqQueue(host, port, user, password)) {
      queue.publish(TASK_QUEUE_NAME, message);
      Assertions.assertDoesNotThrow(() -> queue.deleteQueue(TASK_QUEUE_NAME));
    }
  }

  @Test
  void testRegisterConsumer() {
    var taskId = "test_id";
    var timeCreated = Instant.now();
    var expectedTask = new TaskDTO();
    expectedTask.setRhs(new double[] {1.1, 2.2});
    expectedTask.setMatrix(new double[][] {{3.3, 4.4}, {5.5, 7.7}});
    expectedTask.setId(taskId);
    expectedTask.setDateTime(timeCreated);
    expectedTask.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    Map<String, Object> headers = new HashMap<>();
    var message = new Message<>(expectedTask, headers, Instant.now());
    try (var queue = new RabbitMqQueue(host, port, user, password)) {
      queue.publish(TASK_QUEUE_NAME, message);
      var actualTask = new AtomicReference<>();

      MessageHandler<TaskDTO> messageHandler =
          (receivedMessage, ack) -> {
            actualTask.set(receivedMessage.payload());
            ack.ack(); // Mark message as processed for the queue
          };

      try (var subscription = queue.subscribe(TASK_QUEUE_NAME, TaskDTO.class, messageHandler)) {
        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> Assertions.assertEquals(expectedTask, actualTask.get()));
      }
    }
  }

  @Test
  void testRegisterConsumerWithoutReadingFromFile() {
    var taskId = "test_id";
    var timeCreated = Instant.now();
    var expectedTask = new TaskDTO();
    expectedTask.setRhs(new double[] {3.5, 2.21});
    expectedTask.setMatrix(new double[][] {{55.3, 8.4}, {5.5, 7.6}});
    expectedTask.setId(taskId);
    expectedTask.setDateTime(timeCreated);
    expectedTask.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    Map<String, Object> headers = new HashMap<>();
    var message = new Message<>(expectedTask, headers, Instant.now());
    var properties = new RabbitMqProperties(host, port, user, password);
    try (var queue = new RabbitMqQueue(properties)) {
      queue.publish(TASK_QUEUE_NAME, message);
      var actualTask = new AtomicReference<>();

      MessageHandler<TaskDTO> messageHandler =
          (receivedMessage, ack) -> {
            actualTask.set(receivedMessage.payload());
            ack.ack(); // Mark message as processed for the queue
          };

      try (var subscription = queue.subscribe(TASK_QUEUE_NAME, TaskDTO.class, messageHandler)) {
        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> Assertions.assertEquals(expectedTask, actualTask.get()));
      }
    }
  }

  @Test
  void testRegisterConsumerConstructorWithParams() {
    var taskId = "test_id";
    var timeCreated = Instant.now();
    var expectedTask = new TaskDTO();
    expectedTask.setRhs(new double[] {561.1, 52.287});
    expectedTask.setMatrix(new double[][] {{23.3, 147.44}, {5.5, 7.7}});
    expectedTask.setId(taskId);
    expectedTask.setDateTime(timeCreated);
    expectedTask.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    Map<String, Object> headers = new HashMap<>();
    var message = new Message<>(expectedTask, headers, Instant.now());
    try (var queue = new RabbitMqQueue(host, port, user, password)) {
      queue.publish(TASK_QUEUE_NAME, message);
      var actualTask = new AtomicReference<>();

      MessageHandler<TaskDTO> messageHandler =
          (receivedMessage, ack) -> {
            actualTask.set(receivedMessage.payload());
            ack.ack(); // Mark message as processed for the queue
          };

      try (var subscription = queue.subscribe(TASK_QUEUE_NAME, TaskDTO.class, messageHandler)) {
        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> Assertions.assertEquals(expectedTask, actualTask.get()));
      }
    }
  }
}

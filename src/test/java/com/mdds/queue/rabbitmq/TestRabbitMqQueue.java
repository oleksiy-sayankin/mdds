/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import static com.mdds.queue.rabbitmq.RabbitMqHelper.readFromResources;
import static com.mdds.queue.rabbitmq.RabbitMqProperties.*;
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
import org.junit.jupiter.api.Test;

class TestRabbitMqQueue {

  private static final String TASK_QUEUE_NAME = "task_queue";

  @Test
  void testNoConfFileExists() {
    String randomHost = "random.host";
    int randomPort = 89798;
    String randomUser = "random";
    String randomPassword = "random";
    assertThrows(
        RabbitMqConnectionException.class,
        () -> {
          try (RabbitMqQueue queue =
              new RabbitMqQueue(randomHost, randomPort, randomUser, randomPassword)) {
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
    expectedTask.setSlaeSolvingMethod("test_solving_method");
    Map<String, Object> headers = new HashMap<>();
    Message<TaskDTO> message = new Message<>(expectedTask, headers, Instant.now());
    try (RabbitMqQueue queue = new RabbitMqQueue(readFromResources(DEFAULT_PROPERTIES_FILE))) {
      queue.publish(TASK_QUEUE_NAME, message);
      Assertions.assertDoesNotThrow(() -> queue.publish(TASK_QUEUE_NAME, message));
    }
  }

  @Test
  void testPublishWithException() throws IOException {
    Channel mockChannel = mock(Channel.class);
    doThrow(new IOException("Simulated failure"))
        .when(mockChannel)
        .basicPublish(anyString(), anyString(), any(), any());
    Message<String> message = new Message<>("payload", Map.of(), Instant.now());
    try (RabbitMqQueue queue = new RabbitMqQueue(readFromResources(DEFAULT_PROPERTIES_FILE))) {
      queue.setChannel(mockChannel);
      assertThrows(
          RabbitMqConnectionException.class, () -> queue.publish(TASK_QUEUE_NAME, message));
    }
  }

  @Test
  void testNoConnectionToRabbitMq() {
    RabbitMqProperties properties = readFromResources("no.connection.rabbitmq.properties");
    assertThrows(
        RabbitMqConnectionException.class,
        () -> {
          try (RabbitMqQueue queue = new RabbitMqQueue(properties)) {
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
    expectedTask.setSlaeSolvingMethod("test_solving_method");
    Map<String, Object> headers = new HashMap<>();
    Message<TaskDTO> message = new Message<>(expectedTask, headers, Instant.now());
    try (RabbitMqQueue queue = new RabbitMqQueue(readFromResources(DEFAULT_PROPERTIES_FILE))) {
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
    expectedTask.setSlaeSolvingMethod("test_solving_method");
    Map<String, Object> headers = new HashMap<>();
    Message<TaskDTO> message = new Message<>(expectedTask, headers, Instant.now());
    try (RabbitMqQueue queue = new RabbitMqQueue(readFromResources(DEFAULT_PROPERTIES_FILE))) {
      queue.publish(TASK_QUEUE_NAME, message);
      AtomicReference<TaskDTO> actualTask = new AtomicReference<>();

      MessageHandler<TaskDTO> messageHandler =
          (receivedMessage, ack) -> {
            actualTask.set(receivedMessage.payload());
            ack.ack(); // Mark message as processed for the queue
          };

      try (Subscription subscription =
          queue.subscribe(TASK_QUEUE_NAME, TaskDTO.class, messageHandler)) {
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
    expectedTask.setSlaeSolvingMethod("test_solving_method");
    Map<String, Object> headers = new HashMap<>();
    Message<TaskDTO> message = new Message<>(expectedTask, headers, Instant.now());
    var properties =
        new RabbitMqProperties(
            DEFAULT_HOST, DEFAULT_PORT, DEFAULT_USER, new String(DEFAULT_PASSWORD));
    try (RabbitMqQueue queue = new RabbitMqQueue(properties)) {
      queue.publish(TASK_QUEUE_NAME, message);
      AtomicReference<TaskDTO> actualTask = new AtomicReference<>();

      MessageHandler<TaskDTO> messageHandler =
          (receivedMessage, ack) -> {
            actualTask.set(receivedMessage.payload());
            ack.ack(); // Mark message as processed for the queue
          };

      try (Subscription subscription =
          queue.subscribe(TASK_QUEUE_NAME, TaskDTO.class, messageHandler)) {
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
    expectedTask.setSlaeSolvingMethod("test_solving_method");
    Map<String, Object> headers = new HashMap<>();
    Message<TaskDTO> message = new Message<>(expectedTask, headers, Instant.now());
    try (RabbitMqQueue queue =
        new RabbitMqQueue(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_USER, new String(DEFAULT_PASSWORD))) {
      queue.publish(TASK_QUEUE_NAME, message);
      AtomicReference<TaskDTO> actualTask = new AtomicReference<>();

      MessageHandler<TaskDTO> messageHandler =
          (receivedMessage, ack) -> {
            actualTask.set(receivedMessage.payload());
            ack.ack(); // Mark message as processed for the queue
          };

      try (Subscription subscription =
          queue.subscribe(TASK_QUEUE_NAME, TaskDTO.class, messageHandler)) {
        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> Assertions.assertEquals(expectedTask, actualTask.get()));
      }
    }
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.mdds.queue.*;
import dto.TaskDTO;
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
    assertThrows(
        RabbitMqConnectionException.class,
        () -> {
          try (Queue queue = RabbitMqQueue.newQueue("wrong.file.name")) {
            // Do nothing.
          }
        });
  }

  @Test
  void testNoConnectionToRabbitMq() {
    assertThrows(
        RabbitMqConnectionException.class,
        () -> {
          try (Queue queue = RabbitMqQueue.newQueue("no.connection.rabbitmq.properties")) {
            // Do nothing.
          }
        });
  }

  @Test
  void testPublish() {
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
    try (Queue queue = RabbitMqQueue.newQueue()) {
      queue.publish(TASK_QUEUE_NAME, message);
      Assertions.assertDoesNotThrow(() -> queue.publish(TASK_QUEUE_NAME, message));
    }
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
    try (Queue queue = RabbitMqQueue.newQueue()) {
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
    try (Queue queue = RabbitMqQueue.newQueue()) {
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

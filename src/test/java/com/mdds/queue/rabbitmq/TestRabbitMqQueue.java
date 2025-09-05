/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import com.mdds.queue.rebbitmq.RabbitMqQueue;
import dto.TaskDTO;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestRabbitMqQueue {
  @Test
  void testConnection() {
    var taskQueue = RabbitMqQueue.getInstance();
    Assertions.assertDoesNotThrow(taskQueue::connect);
  }

  @Test
  void testPublish() {
    var taskQueueName = "task_queue";
    var taskQueue = RabbitMqQueue.getInstance();
    taskQueue.connect();
    taskQueue.declareQueue(taskQueueName);
    var taskId = "test_id";
    var timeCreated = Instant.now();
    var expectedTask = new TaskDTO();
    expectedTask.setRhs(new double[] {3.4, 4.6});
    expectedTask.setMatrix(new double[][] {{3.7, 5.6}, {2.9, 4.5}});
    expectedTask.setId(taskId);
    expectedTask.setDateTime(timeCreated);
    expectedTask.setSLAESolvingMethod("test_solving_method");
    Assertions.assertDoesNotThrow(
        () -> {
          taskQueue.publish(expectedTask, taskQueueName);
        });
    taskQueue.close();
  }

  @Test
  void testDeclareQueue() {
    var taskQueueName = "task_queue";
    var taskQueue = RabbitMqQueue.getInstance();
    taskQueue.connect();
    Assertions.assertDoesNotThrow(
        () -> {
          taskQueue.declareQueue(taskQueueName);
        });
    taskQueue.close();
  }

  @Test
  void testClose() {
    var taskQueue = RabbitMqQueue.getInstance();
    taskQueue.connect();
    Assertions.assertDoesNotThrow(taskQueue::close);
  }
}

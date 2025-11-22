/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.result;

import static com.mdds.common.util.CommonHelper.findFreePort;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mdds.common.AppConstants;
import com.mdds.common.AppConstantsFactory;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.TaskStatus;
import com.mdds.queue.Message;
import com.mdds.queue.Queue;
import com.mdds.queue.rabbitmq.RabbitMqQueueProvider;
import com.mdds.storage.DataStorage;
import com.mdds.storage.redis.RedisStorageProvider;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;
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
class TestResultConsumerService {
  private static final int REDIS_SERVER_PORT = findFreePort();
  private static RedisServer redisServer;
  private static DataStorage storage;
  private static Queue queue;

  @Container
  private static final RabbitMQContainer rabbitMq =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672);

  @BeforeAll
  static void startServer() throws IOException {
    redisServer = new RedisServer(REDIS_SERVER_PORT);
    redisServer.start();
    System.setProperty("redis.host", "localhost");
    System.setProperty("redis.port", String.valueOf(REDIS_SERVER_PORT));
    System.setProperty("rabbitmq.host", rabbitMq.getHost());
    System.setProperty("rabbitmq.port", String.valueOf(rabbitMq.getAmqpPort()));
    System.setProperty("rabbitmq.user", rabbitMq.getAdminUsername());
    System.setProperty("rabbitmq.password", rabbitMq.getAdminPassword());
    storage = new RedisStorageProvider().get();
    queue = new RabbitMqQueueProvider().get();
  }

  @AfterAll
  static void stopServer() throws IOException {
    if (redisServer != null) {
      redisServer.stop();
    }
    if (storage != null) {
      storage.close();
    }
    if (queue != null) {
      queue.close();
    }
  }

  @Test
  void testResultConsumerService() {
    var resultConsumerService = new ResultConsumerService(storage, queue);
    // Prepare and put data to result queue
    var taskId = UUID.randomUUID().toString();
    var expectedResult =
        new ResultDTO(
            taskId,
            Instant.now(),
            Instant.now(),
            TaskStatus.DONE,
            100,
            new double[] {1.1, 2.2, 3.3, 4.4},
            "");
    var message = new Message<>(expectedResult, new HashMap<>(), Instant.now());
    queue.publish(AppConstantsFactory.getString(AppConstants.RESULT_QUEUE_NAME), message);
    // Read result from Data Storage
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              var actualResult = storage.get(taskId, ResultDTO.class);
              assertTrue(actualResult.isPresent());
              actualResult.ifPresent(ar -> assertEquals(expectedResult, ar));
            });
    resultConsumerService.close();
  }
}

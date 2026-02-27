/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

import com.mdds.dto.CancelJobDTO;
import com.mdds.queue.CancelDestinationResolver;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
class TestRabbitMqBus {

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
  void testSendCancel() {
    var jobId = "test_job_id";
    var executorId = "test_executor_id";
    var cancelJobDTO = new CancelJobDTO(jobId);
    Map<String, Object> headers = new HashMap<>();
    var message = new Message<>(cancelJobDTO, headers, Instant.now());
    try (var queue = new RabbitMqQueue(host, port, user, password)) {
      var bus = new RabbitMqCancelBus(queue, new CancelDestinationResolver());
      assertThatCode(() -> bus.sendCancel(executorId, message)).doesNotThrowAnyException();
    }
  }

  @Test
  void testSubscribe() {
    var jobId = "test_job_id";
    var executorId = "test_executor_id";
    var cancelJobDTO = new CancelJobDTO(jobId);
    Map<String, Object> headers = new HashMap<>();
    var message = new Message<>(cancelJobDTO, headers, Instant.now());

    try (var queue = new RabbitMqQueue(host, port, user, password)) {
      var bus = new RabbitMqCancelBus(queue, new CancelDestinationResolver());
      bus.sendCancel(executorId, message);
      var actualJob = new AtomicReference<>();

      MessageHandler<CancelJobDTO> messageHandler =
          (receivedMessage, ack) -> {
            actualJob.set(receivedMessage.payload());
            ack.ack(); // Mark message as processed for the queue
          };

      try (var ignore = bus.subscribe(executorId, messageHandler)) {
        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> assertThat(actualJob.get()).isEqualTo(cancelJobDTO));
      }
    }
  }
}

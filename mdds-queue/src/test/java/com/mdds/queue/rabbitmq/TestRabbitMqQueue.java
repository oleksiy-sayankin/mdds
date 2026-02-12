/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import static com.mdds.dto.SlaeSolver.NUMPY_EXACT_SOLVER;
import static com.mdds.queue.rabbitmq.RabbitMqHelper.readFromResources;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mdds.dto.JobDTO;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
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
class TestRabbitMqQueue {
  private static final String JOB_QUEUE_NAME = "job_queue";

  @Container
  private static final RabbitMQContainer rabbitMq =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672);

  private static String host;
  private static int port;
  private static String user;
  private static String password;
  private static int maxInboundMessageBodySize = 67_108_864;

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
    var timeOut = Duration.ofSeconds(1);
    assertThatThrownBy(
            () -> {
              try (var queue =
                  new RabbitMqQueue(randomHost, randomPort, randomUser, randomPassword, timeOut)) {
                // Do nothing.
              }
            })
        .isInstanceOf(RabbitMqConnectionException.class)
        .hasMessageContaining("Failed to connect to rabbitmq://random.host:89798");
  }

  @Test
  void testCreateChannelException() throws IOException {
    try (var connection = mock(Connection.class)) {
      when(connection.createChannel()).thenThrow(new IOException());
      assertThatThrownBy(
              () -> {
                try (var ignore = new RabbitMqQueue(connection)) {
                  // Do nothing.
                }
              })
          .isInstanceOf(RabbitMqConnectionException.class)
          .hasMessageContaining("Failed to create RabbitMq connection");
    }
  }

  @Test
  void testPublishException() throws IOException {
    var jobId = "test_id";
    var timeCreated = Instant.now();
    var expectedJob = new JobDTO();
    expectedJob.setRhs(new double[] {73.4, 764.6});
    expectedJob.setMatrix(new double[][] {{783.7, 757.6}, {72.9, 4.75}});
    expectedJob.setId(jobId);
    expectedJob.setDateTime(timeCreated);
    expectedJob.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    Map<String, Object> headers = new HashMap<>();
    var message = new Message<>(expectedJob, headers, Instant.now());
    try (var connection = mock(Connection.class)) {
      var channel = mock(Channel.class);
      doThrow(new IOException()).when(channel).basicPublish(anyString(), anyString(), any(), any());
      try (var queue = new RabbitMqQueue(channel, connection)) {
        assertThatThrownBy(() -> queue.publish(JOB_QUEUE_NAME, message))
            .isInstanceOf(RabbitMqConnectionException.class)
            .hasMessageContaining("Failed to publish to queue");
      }
    }
  }

  @Test
  void testPublish() {
    var jobId = "test_id";
    var timeCreated = Instant.now();
    var expectedJob = new JobDTO();
    expectedJob.setRhs(new double[] {73.4, 764.6});
    expectedJob.setMatrix(new double[][] {{783.7, 757.6}, {72.9, 4.75}});
    expectedJob.setId(jobId);
    expectedJob.setDateTime(timeCreated);
    expectedJob.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    Map<String, Object> headers = new HashMap<>();
    var message = new Message<>(expectedJob, headers, Instant.now());
    try (var queue = new RabbitMqQueue(host, port, user, password)) {
      queue.publish(JOB_QUEUE_NAME, message);
      assertThatCode(() -> queue.publish(JOB_QUEUE_NAME, message)).doesNotThrowAnyException();
    }
  }

  @Test
  void testWrongUserAndPassword() {
    var timeOut = Duration.ofSeconds(1);
    assertThatThrownBy(
            () -> {
              try (var ignore =
                  new RabbitMqQueue(host, port, "wrong user", "wrong password", timeOut)) {
                // Do nothing.
              }
            })
        .isInstanceOf(RabbitMqConnectionException.class)
        .hasMessageContaining("Failed to connect to rabbitmq:");
  }

  @Test
  void testNoConnectionToRabbitMq() {
    var timeOut = Duration.ofSeconds(1);
    var properties = readFromResources("no.connection.rabbitmq.properties");
    assertThatThrownBy(
            () -> {
              try (var ignore = new RabbitMqQueue(properties, timeOut)) {
                // Do nothing.
              }
            })
        .isInstanceOf(RabbitMqConnectionException.class)
        .hasMessageContaining("Failed to connect to rabbitmq://wrong.host:7974");
  }

  @Test
  void testDeleteQueue() {
    var jobId = "test_id";
    var timeCreated = Instant.now();
    var expectedJob = new JobDTO();
    expectedJob.setRhs(new double[] {3.4, 4.6});
    expectedJob.setMatrix(new double[][] {{3.7, 5.6}, {2.9, 4.5}});
    expectedJob.setId(jobId);
    expectedJob.setDateTime(timeCreated);
    expectedJob.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    Map<String, Object> headers = new HashMap<>();
    var message = new Message<>(expectedJob, headers, Instant.now());
    try (var queue = new RabbitMqQueue(host, port, user, password)) {
      queue.publish(JOB_QUEUE_NAME, message);
      assertThatCode(() -> queue.deleteQueue(JOB_QUEUE_NAME)).doesNotThrowAnyException();
    }
  }

  @Test
  void testFailedToDelete() throws IOException {
    var jobId = "test_id";
    var timeCreated = Instant.now();
    var expectedJob = new JobDTO();
    expectedJob.setRhs(new double[] {3.4, 4.6});
    expectedJob.setMatrix(new double[][] {{3.7, 5.6}, {2.9, 4.5}});
    expectedJob.setId(jobId);
    expectedJob.setDateTime(timeCreated);
    expectedJob.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    Map<String, Object> headers = new HashMap<>();
    var message = new Message<>(expectedJob, headers, Instant.now());
    var channel = mock(Channel.class);
    doThrow(new IOException()).when(channel).queueDelete(anyString());
    var connection = mock(Connection.class);
    try (var queue = new RabbitMqQueue(channel, connection)) {
      queue.publish(JOB_QUEUE_NAME, message);
      assertThatThrownBy(() -> queue.deleteQueue(JOB_QUEUE_NAME))
          .isInstanceOf(RabbitMqConnectionException.class)
          .hasMessageContaining("Failed to delete queue");
    }
  }

  @Test
  void testRegisterConsumer() {
    var jobId = "test_id";
    var timeCreated = Instant.now();
    var expectedJob = new JobDTO();
    expectedJob.setRhs(new double[] {1.1, 2.2});
    expectedJob.setMatrix(new double[][] {{3.3, 4.4}, {5.5, 7.7}});
    expectedJob.setId(jobId);
    expectedJob.setDateTime(timeCreated);
    expectedJob.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    Map<String, Object> headers = new HashMap<>();
    var message = new Message<>(expectedJob, headers, Instant.now());
    try (var queue = new RabbitMqQueue(host, port, user, password)) {
      queue.publish(JOB_QUEUE_NAME, message);
      var actualJob = new AtomicReference<>();

      MessageHandler<JobDTO> messageHandler =
          (receivedMessage, ack) -> {
            actualJob.set(receivedMessage.payload());
            ack.ack(); // Mark message as processed for the queue
          };

      try (var ignore = queue.subscribe(JOB_QUEUE_NAME, JobDTO.class, messageHandler)) {
        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> assertThat(actualJob.get()).isEqualTo(expectedJob));
      }
    }
  }

  @Test
  void testSubscribeFailedToConsume() throws IOException {
    var jobId = "test_id";
    var timeCreated = Instant.now();
    var expectedJob = new JobDTO();
    expectedJob.setRhs(new double[] {1.1, 2.2});
    expectedJob.setMatrix(new double[][] {{3.3, 4.4}, {5.5, 7.7}});
    expectedJob.setId(jobId);
    expectedJob.setDateTime(timeCreated);
    expectedJob.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    Map<String, Object> headers = new HashMap<>();
    var message = new Message<>(expectedJob, headers, Instant.now());

    var connection = mock(Connection.class);
    var channel = mock(Channel.class);
    doThrow(new IOException())
        .when(channel)
        .basicConsume(
            anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));

    try (var queue = new RabbitMqQueue(channel, connection)) {
      queue.publish(JOB_QUEUE_NAME, message);

      MessageHandler<JobDTO> messageHandler =
          (receivedMessage, ack) -> {
            ack.ack(); // Mark message as processed for the queue
          };

      assertThatThrownBy(() -> queue.subscribe(JOB_QUEUE_NAME, JobDTO.class, messageHandler))
          .isInstanceOf(RabbitMqConnectionException.class)
          .hasMessageContaining("Failed consume from queue");
    }
  }

  @Test
  void testSubscribeFailedToDeclare() throws IOException {
    var jobId = "test_id";
    var timeCreated = Instant.now();
    var expectedJob = new JobDTO();
    expectedJob.setRhs(new double[] {1.1, 2.2});
    expectedJob.setMatrix(new double[][] {{3.3, 4.4}, {5.5, 7.7}});
    expectedJob.setId(jobId);
    expectedJob.setDateTime(timeCreated);
    expectedJob.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);

    var connection = mock(Connection.class);
    var channel = mock(Channel.class);
    doThrow(new IOException())
        .when(channel)
        .queueDeclare(
            nullable(String.class), anyBoolean(), anyBoolean(), anyBoolean(), nullable(Map.class));

    try (var queue = new RabbitMqQueue(channel, connection)) {
      MessageHandler<JobDTO> messageHandler =
          (receivedMessage, ack) -> {
            ack.ack(); // Mark message as processed for the queue
          };

      assertThatThrownBy(() -> queue.subscribe(JOB_QUEUE_NAME, JobDTO.class, messageHandler))
          .isInstanceOf(RabbitMqConnectionException.class)
          .hasMessageContaining("Failed to declare queue");
    }
  }

  @Test
  void testSubscribeFailedToCancel() throws IOException {
    var jobId = "test_id";
    var timeCreated = Instant.now();
    var expectedJob = new JobDTO();
    expectedJob.setRhs(new double[] {1.1, 2.2});
    expectedJob.setMatrix(new double[][] {{3.3, 4.4}, {5.5, 7.7}});
    expectedJob.setId(jobId);
    expectedJob.setDateTime(timeCreated);
    expectedJob.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    Map<String, Object> headers = new HashMap<>();
    var message = new Message<>(expectedJob, headers, Instant.now());

    var connection = mock(Connection.class);
    var channel = mock(Channel.class);
    doThrow(new IOException()).when(channel).basicCancel(nullable(String.class));

    try (var queue = new RabbitMqQueue(channel, connection)) {
      queue.publish(JOB_QUEUE_NAME, message);

      MessageHandler<JobDTO> messageHandler =
          (receivedMessage, ack) -> {
            ack.ack(); // Mark message as processed for the queue
          };

      var subscription = queue.subscribe(JOB_QUEUE_NAME, JobDTO.class, messageHandler);
      assertThatThrownBy(subscription::close)
          .isInstanceOf(RabbitMqConnectionException.class)
          .hasMessageContaining("Failed cancel subscription");
    }
  }

  @Test
  void testRegisterConsumerWithoutReadingFromFile() {
    var jobId = "test_id";
    var timeCreated = Instant.now();
    var expectedJob = new JobDTO();
    expectedJob.setRhs(new double[] {3.5, 2.21});
    expectedJob.setMatrix(new double[][] {{55.3, 8.4}, {5.5, 7.6}});
    expectedJob.setId(jobId);
    expectedJob.setDateTime(timeCreated);
    expectedJob.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    Map<String, Object> headers = new HashMap<>();
    var message = new Message<>(expectedJob, headers, Instant.now());
    var properties = new RabbitMqProperties(host, port, user, password, maxInboundMessageBodySize);
    try (var queue = new RabbitMqQueue(properties)) {
      queue.publish(JOB_QUEUE_NAME, message);
      var actualJob = new AtomicReference<>();

      MessageHandler<JobDTO> messageHandler =
          (receivedMessage, ack) -> {
            actualJob.set(receivedMessage.payload());
            ack.ack(); // Mark message as processed for the queue
          };

      try (var ignore = queue.subscribe(JOB_QUEUE_NAME, JobDTO.class, messageHandler)) {
        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> assertThat(actualJob.get()).isEqualTo(expectedJob));
      }
    }
  }

  @Test
  void testRegisterConsumerConstructorWithParams() {
    var jobId = "test_id";
    var timeCreated = Instant.now();
    var expectedJob = new JobDTO();
    expectedJob.setRhs(new double[] {561.1, 52.287});
    expectedJob.setMatrix(new double[][] {{23.3, 147.44}, {5.5, 7.7}});
    expectedJob.setId(jobId);
    expectedJob.setDateTime(timeCreated);
    expectedJob.setSlaeSolvingMethod(NUMPY_EXACT_SOLVER);
    Map<String, Object> headers = new HashMap<>();
    var message = new Message<>(expectedJob, headers, Instant.now());
    try (var queue = new RabbitMqQueue(host, port, user, password)) {
      queue.publish(JOB_QUEUE_NAME, message);
      var actualJob = new AtomicReference<>();

      MessageHandler<JobDTO> messageHandler =
          (receivedMessage, ack) -> {
            actualJob.set(receivedMessage.payload());
            ack.ack(); // Mark message as processed for the queue
          };

      try (var ignore = queue.subscribe(JOB_QUEUE_NAME, JobDTO.class, messageHandler)) {
        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> assertThat(actualJob.get()).isEqualTo(expectedJob));
      }
    }
  }
}

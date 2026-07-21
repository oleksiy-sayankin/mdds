/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import static com.mdds.queue.rabbitmq.RabbitMqHelper.readFromResources;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
class TestRabbitMqQueueClient {

  @Container
  private static final RabbitMQContainer rabbitMq =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672);

  private static String host;
  private static int port;

  @BeforeAll
  static void init() {
    host = rabbitMq.getHost();
    port = rabbitMq.getAmqpPort();
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
                  new RabbitMqQueueClient(
                      randomHost, randomPort, randomUser, randomPassword, timeOut)) {
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
                try (var ignore = new RabbitMqQueueClient(connection)) {
                  // Do nothing.
                }
              })
          .isInstanceOf(RabbitMqConnectionException.class)
          .hasMessageContaining("Failed to create RabbitMq connection");
    }
  }

  @Test
  void testWrongUserAndPassword() {
    var timeOut = Duration.ofSeconds(1);
    assertThatThrownBy(
            () -> {
              try (var ignore =
                  new RabbitMqQueueClient(host, port, "wrong user", "wrong password", timeOut)) {
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
              try (var ignore = new RabbitMqQueueClient(properties, timeOut)) {
                // Do nothing.
              }
            })
        .isInstanceOf(RabbitMqConnectionException.class)
        .hasMessageContaining("Failed to connect to rabbitmq://wrong.host:7974");
  }
}

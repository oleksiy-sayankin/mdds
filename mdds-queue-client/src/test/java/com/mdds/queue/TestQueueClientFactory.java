/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue;

import static com.mdds.queue.rabbitmq.RabbitMqHelper.readFromResources;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.mdds.queue.rabbitmq.RabbitMqConnectionException;
import com.mdds.queue.rabbitmq.RabbitMqQueueClient;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
class TestQueueClientFactory {

  @Container
  private static final RabbitMQContainer rabbitMq =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672);

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
  void testNoConnectionToRabbitMq() {
    var properties = readFromResources("no.connection.rabbitmq.properties");
    var timeOut = Duration.ofSeconds(1);
    assertThatThrownBy(
            () -> {
              try (var queue = new RabbitMqQueueClient(properties, timeOut)) {
                // Do nothing.
              }
            })
        .isInstanceOf(RabbitMqConnectionException.class)
        .hasMessageContaining("Failed to connect to rabbitmq://wrong.host:7974");
  }
}

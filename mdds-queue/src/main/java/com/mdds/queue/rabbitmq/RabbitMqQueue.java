/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.mdds.common.util.JsonHelper;
import com.mdds.queue.Acknowledger;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import com.mdds.queue.Queue;
import com.mdds.queue.Subscription;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;

/** Queue that delivers tasks to Executors. */
@Slf4j
public class RabbitMqQueue implements Queue {
  private final @Nonnull Channel channel;
  private final @Nonnull Connection connection;

  public RabbitMqQueue(@Nonnull RabbitMqProperties conf) {
    this(conf.getHost(), conf.getPort(), conf.getUser(), conf.getPassword());
  }

  public RabbitMqQueue(@Nonnull RabbitMqProperties conf, Duration timeOut) {
    this(conf.getHost(), conf.getPort(), conf.getUser(), conf.getPassword(), timeOut);
  }

  public RabbitMqQueue(@Nonnull String host, int port, String user, String password) {
    this(host, port, user, password, Duration.ofSeconds(60));
  }

  public RabbitMqQueue(
      @Nonnull String host, int port, String user, String password, Duration timeOut) {
    this(
        createConnectionWithRetry(
            createConnectionFactory(host, port, user, password), host, port, timeOut));
  }

  public RabbitMqQueue(@Nonnull Connection connection) {
    try {
      this.connection = connection;
      log.info("Connected to RabbitMq {}", connection);
      channel = connection.createChannel();
      log.info("Created RabbitMq channel {}", channel);
    } catch (IOException e) {
      throw new RabbitMqConnectionException("Failed to create RabbitMq connection", e);
    }
  }

  @VisibleForTesting
  public RabbitMqQueue(@Nonnull Channel channel, @Nonnull Connection connection) {
    this.channel = channel;
    this.connection = connection;
  }

  /**
   * Converts Map to AMQP.BasicProperties.
   *
   * @param headers Map with parameters.
   * @return Equivalent of the input map but as AMQP.BasicProperties
   */
  @VisibleForTesting
  static @Nonnull AMQP.BasicProperties convertFrom(@Nonnull Map<String, Object> headers) {
    return new AMQP.BasicProperties.Builder().headers(headers).build();
  }

  @Override
  public <T> void publish(@Nonnull String queueName, @Nonnull Message<T> message) {
    declareQueue(queueName);
    try {
      channel.basicPublish(
          "",
          queueName,
          convertFrom(message.headers()),
          JsonHelper.toJson(message.payload()).getBytes());
    } catch (IOException e) {
      throw new RabbitMqConnectionException("Failed to publish to queue: " + queueName, e);
    }
  }

  @Override
  public <T> @Nonnull Subscription subscribe(
      @Nonnull String queueName,
      @Nonnull Class<T> payloadType,
      @Nonnull MessageHandler<T> handler) {
    declareQueue(queueName);
    String tag;
    DeliverCallback deliverCallback =
        (consumerTag, delivery) -> {
          T payload = JsonHelper.fromJson(new String(delivery.getBody(), UTF_8), payloadType);
          var message =
              new Message<>(payload, delivery.getProperties().getHeaders(), Instant.now());

          Acknowledger acknowledger =
              new Acknowledger() {
                @Override
                public void ack() {
                  // Acknowledge the message to RabbitMQ
                  try {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                  } catch (IOException e) {
                    throw new RabbitMqConnectionException(
                        "Failed to acknowledge to queue: " + queueName, e);
                  }
                }

                @Override
                public void nack(boolean requeue) {
                  try {
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, requeue);
                  } catch (IOException e) {
                    throw new RabbitMqConnectionException(
                        "Failed to reject message from queue: " + queueName, e);
                  }
                }
              };
          handler.handle(message, acknowledger);
        };

    CancelCallback cancelCallback =
        consumerTag -> {
          // Do nothing
        };
    try {
      tag =
          channel.basicConsume(
              queueName,
              false,
              deliverCallback,
              cancelCallback); // 'false' for manual acknowledgment
    } catch (IOException e) {
      throw new RabbitMqConnectionException("Failed consume from queue: " + queueName, e);
    }

    return () -> {
      try {
        channel.basicCancel(tag);
      } catch (IOException e) {
        throw new RabbitMqConnectionException(
            "Failed cancel subscription '" + queueName + "', consumer tag '" + tag + "'", e);
      }
    };
  }

  @Override
  public void deleteQueue(@Nonnull String queueName) {
    try {
      channel.queueDelete(queueName);
    } catch (IOException e) {
      throw new RabbitMqConnectionException("Failed to delete queue: " + queueName, e);
    }
  }

  @Override
  public void close() {
    try {
      if (channel.isOpen()) channel.close();
      log.info("Closed queue channel {}", channel);
    } catch (Exception e) {
      log.warn("Failed to close channel", e);
    } finally {
      try {
        if (connection.isOpen()) connection.close();
      } catch (Exception e) {
        log.warn("Failed to close connection", e);
      }
    }
  }

  @Override
  public String toString() {
    return getClass().getName() + "[" + connection.getAddress() + ":" + connection.getPort() + "]";
  }

  private void declareQueue(@Nonnull String queueName) {
    // Declare a queue (idempotent - creates if it doesn't exist)
    try {
      channel.queueDeclare(queueName, false, false, false, null);
    } catch (IOException e) {
      throw new RabbitMqConnectionException("Failed to declare queue: " + queueName, e);
    }
  }

  private static @Nonnull Connection createConnectionWithRetry(
      ConnectionFactory factory, String host, int port, Duration timeOut) {
    try {
      return Objects.requireNonNull(
          Awaitility.await()
              .atMost(timeOut)
              .pollInterval(Duration.ofSeconds(2))
              .pollDelay(Duration.ZERO)
              .ignoreExceptions()
              .until(
                  () -> {
                    try {
                      log.info("Connecting to rabbitmq://{}:{}...", host, port);
                      var connection = factory.newConnection();
                      if (connection.isOpen()) {
                        log.info("Connected to RabbitMQ {}:{}", host, port);
                        return connection;
                      } else {
                        log.warn("Connection not open yet, retrying...");
                        connection.close();
                        return null;
                      }
                    } catch (IOException | TimeoutException e) {
                      log.warn("RabbitMQ not ready at {}:{}, retrying...", host, port);
                      return null;
                    }
                  },
                  connection -> connection != null && connection.isOpen()));
    } catch (ConditionTimeoutException e) {
      throw new RabbitMqConnectionException(
          "Failed to connect to rabbitmq://" + host + ":" + port, e);
    }
  }

  private static @Nonnull ConnectionFactory createConnectionFactory(
      @Nonnull String host, int port, String user, String password) {
    var factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername(user);
    factory.setPassword(password);
    return factory;
  }
}

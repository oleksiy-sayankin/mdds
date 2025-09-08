/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.mdds.queue.*;
import com.mdds.util.JsonHelper;
import com.rabbitmq.client.*;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Queue that delivers tasks to Executors. */
public class RabbitMqQueue implements Queue {
  private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqQueue.class);
  private Channel channel;
  private Connection connection;

  public RabbitMqQueue(RabbitMqProperties properties) {
    connect(properties.host(), properties.port(), properties.user(), properties.password());
  }

  public RabbitMqQueue(String host, int port, String user, String password) {
    connect(host, port, user, password);
  }

  @Override
  public <T> void publish(String queueName, Message<T> message) {
    declareQueue(queueName);
    try {
      channel.basicPublish(
          "",
          queueName,
          RabbitMqHelper.convertFrom(message.headers()),
          JsonHelper.toJson(message.payload()).getBytes());
    } catch (IOException e) {
      throw new RabbitMqConnectionException("Failed to publish to queue: " + queueName, e);
    }
  }

  @Override
  public <T> Subscription subscribe(
      String queueName, Class<T> payloadType, MessageHandler<T> handler) {
    String tag;
    DeliverCallback deliverCallback =
        (consumerTag, delivery) -> {
          T payload = JsonHelper.fromJson(new String(delivery.getBody(), UTF_8), payloadType);
          Message<T> message =
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
  public void deleteQueue(String queueName) {
    try {
      channel.queueDelete(queueName);
    } catch (IOException e) {
      throw new RabbitMqConnectionException("Failed to delete queue: " + queueName, e);
    }
  }

  @Override
  public void close() {
    try {
      if (channel != null && channel.isOpen()) channel.close();
    } catch (Exception e) {
      LOGGER.warn("Failed to close channel", e);
    } finally {
      try {
        if (connection != null && connection.isOpen()) connection.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close connection", e);
      }
    }
  }

  @VisibleForTesting
  void setChannel(Channel channel) {
    this.channel = channel;
  }

  private void declareQueue(String queueName) {
    // Declare a queue (idempotent - creates if it doesn't exist)
    try {
      channel.queueDeclare(queueName, false, false, false, null);
    } catch (IOException e) {
      throw new RabbitMqConnectionException("Failed to declare queue: " + queueName, e);
    }
  }

  private void connect(String host, int port, String user, String password) {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername(user);
    factory.setPassword(password);

    try {
      connection = factory.newConnection();
      channel = connection.createChannel();
    } catch (IOException | TimeoutException e) {
      throw new RabbitMqConnectionException("Failed to create RabbitMq connection", e);
    }
  }
}

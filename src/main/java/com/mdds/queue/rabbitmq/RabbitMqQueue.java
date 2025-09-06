/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.mdds.queue.*;
import com.mdds.util.JsonHelper;
import com.rabbitmq.client.*;
import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Queue that delivers tasks to Executors. */
public class RabbitMqQueue implements Queue {
  private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqQueue.class);

  private RabbitMqQueue(String rabbitMqConfFileName) {
    connect(rabbitMqConfFileName);
  }

  public static Queue newQueue() {
    return new RabbitMqQueue("rabbitmq.properties");
  }

  public static Queue newQueue(String rabbitMqConfFileName) {
    return new RabbitMqQueue(rabbitMqConfFileName);
  }

  private Channel channel;
  private Connection connection;

  private void connect(String rabbitMqConfFileName) {
    ConnectionFactory factory = new ConnectionFactory();

    var properties = new Properties();
    try (var input = getClass().getClassLoader().getResourceAsStream(rabbitMqConfFileName)) {
      if (input == null) {
        LOGGER.error("rabbitmq.properties not found in resources");
        throw new RabbitMqConnectionException();
      }
      properties.load(input);
    } catch (IOException e) {
      LOGGER.error("Could not load rabbitmq.properties file.");
      throw new RabbitMqConnectionException(e);
    }
    var host =
        System.getProperty("rabbitmq.host", properties.getProperty("rabbitmq.host", "localhost"));
    int port =
        Integer.parseInt(
            System.getProperty("rabbitmq.port", properties.getProperty("rabbitmq.port", "5672")));
    var user =
        System.getProperty(
            "rabbitmq.user.name", properties.getProperty("rabbitmq.user.name", "guest"));
    var password =
        System.getProperty(
            "rabbitmq.user.password", properties.getProperty("rabbitmq.user.password", "guest"));

    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername(user);
    factory.setPassword(password);

    try {
      connection = factory.newConnection();
      channel = connection.createChannel();
    } catch (IOException | TimeoutException e) {
      LOGGER.error("Failed to create RabbitMq connection");
      throw new RabbitMqConnectionException(e);
    }
  }

  private void declareQueue(String queueName) {
    // Declare a queue (idempotent - creates if it doesn't exist)
    try {
      channel.queueDeclare(queueName, false, false, false, null);
    } catch (IOException e) {
      LOGGER.error("Failed to declare queue {}", queueName);
      throw new RabbitMqConnectionException(e);
    }
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
      LOGGER.error("Failed to publish to queue {}", queueName);
      throw new RabbitMqConnectionException(e);
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
                    LOGGER.error("Failed to acknowledge to queue {}", queueName);
                    throw new RabbitMqConnectionException(e);
                  }
                }

                @Override
                public void nack(boolean requeue) {
                  try {
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, requeue);
                  } catch (IOException e) {
                    LOGGER.error("Failed to reject message from queue {}", queueName);
                    throw new RabbitMqConnectionException(e);
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
      LOGGER.error("Failed consume from queue {}", queueName);
      throw new RabbitMqConnectionException(e);
    }

    return () -> {
      try {
        channel.basicCancel(tag);
      } catch (IOException e) {
        LOGGER.error("Failed cancel subscription '{}', consumer tag '{}'", queueName, tag);
        throw new RabbitMqConnectionException(e);
      }
    };
  }

  @Override
  public void deleteQueue(String queueName) {
    try {
      channel.queueDelete(queueName);
    } catch (IOException e) {
      LOGGER.error("Failed to delete queue {}", queueName);
      throw new RabbitMqConnectionException(e);
    }
  }

  @Override
  public void close() {
    try {
      if (channel != null && channel.isOpen()) channel.close();
      if (connection != null && connection.isOpen()) {
        connection.close();
      }
    } catch (IOException | TimeoutException e) {
      LOGGER.error("Failed to close channel or connection.");
      throw new RabbitMqConnectionException(e);
    }
  }
}

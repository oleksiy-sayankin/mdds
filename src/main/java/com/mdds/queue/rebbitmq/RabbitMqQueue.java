/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rebbitmq;

import com.mdds.queue.Queue;
import com.mdds.util.JsonHelper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Queue that delivers tasks to Executors. */
public class RabbitMqQueue implements Queue {
  private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqQueue.class);

  private RabbitMqQueue() {}

  private static final Queue TASK_QUEUE = new RabbitMqQueue();

  public static Queue getInstance() {
    return TASK_QUEUE;
  }

  private Channel channel;
  private Connection connection;

  @Override
  public void connect() {
    ConnectionFactory factory = new ConnectionFactory();

    var properties = new Properties();
    try (var input = getClass().getClassLoader().getResourceAsStream("rabbitmq.properties")) {
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
      throw new RabbitMqConnectionException(e);
    }
  }

  @Override
  public void declareQueue(String queueName) {
    // Declare a queue (idempotent - creates if it doesn't exist)
    try {
      channel.queueDeclare(queueName, false, false, false, null);
    } catch (IOException e) {
      throw new RabbitMqConnectionException(e);
    }
  }

  @Override
  public void publish(Object task, String queueName) {
    try {
      channel.basicPublish("", queueName, null, JsonHelper.toJson(task).getBytes());
    } catch (IOException e) {
      throw new RabbitMqConnectionException(e);
    }
  }

  @Override
  public void close() {
    try {
      channel.close();
      connection.close();
    } catch (IOException | TimeoutException e) {
      throw new RabbitMqConnectionException(e);
    }
  }
}

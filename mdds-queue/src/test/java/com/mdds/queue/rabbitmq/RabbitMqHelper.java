/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import java.io.IOException;
import java.util.Properties;

/** Utility method for RabbitMq. */
public final class RabbitMqHelper {
  private RabbitMqHelper() {}

  private static final String DEFAULT_HOST = "localhost444";
  private static final int DEFAULT_PORT = 5672;
  private static final String DEFAULT_USER = "guest444";
  private static final char[] DEFAULT_PASSWORD = {'g', 'u', 'e', 's', 't'};
  private static final int DEFAULT_MAX_INBOUND_MESSAGE_BODY_SIZE = 67_108_864;

  /**
   * Reads RabbitMq connection parameters from properties file in classpath. File is searched inside
   * *.jar archive in its root folder.
   *
   * @param rabbitMqProperties *.properties file inside *.jar file which is in classpath.
   * @return record with connection parameters
   */
  public static RabbitMqProperties readFromResources(String rabbitMqProperties) {
    var properties = new Properties();
    try (var input =
        RabbitMqHelper.class.getClassLoader().getResourceAsStream(rabbitMqProperties)) {
      if (input == null) {
        throw new RabbitMqConnectionException("File not found in resources: " + rabbitMqProperties);
      }
      properties.load(input);
    } catch (IOException e) {
      throw new RabbitMqConnectionException("Could not load file: " + rabbitMqProperties, e);
    }
    var host =
        System.getProperty("rabbitmq.host", properties.getProperty("rabbitmq.host", DEFAULT_HOST));
    int port =
        Integer.parseInt(
            System.getProperty(
                "rabbitmq.port",
                properties.getProperty("rabbitmq.port", String.valueOf(DEFAULT_PORT))));
    var user =
        System.getProperty(
            "rabbitmq.user.name", properties.getProperty("rabbitmq.user.name", DEFAULT_USER));
    var password =
        System.getProperty(
            "rabbitmq.user.password",
            properties.getProperty("rabbitmq.user.password", new String(DEFAULT_PASSWORD)));
    var maxInboundMessageBodySize =
        Integer.parseInt(
            System.getProperty(
                "rabbitmq.max.inbound.message.body.size",
                properties.getProperty(
                    "rabbitmq.max.inbound.message.body.size",
                    String.valueOf(DEFAULT_MAX_INBOUND_MESSAGE_BODY_SIZE))));
    return new RabbitMqProperties(host, port, user, password, maxInboundMessageBodySize);
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import static com.mdds.queue.rabbitmq.RabbitMqConf.*;

import com.rabbitmq.client.AMQP;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/** Utility method for RabbitMq. */
public final class RabbitMqHelper {
  private RabbitMqHelper() {}

  /**
   * Converts Map to AMQP.BasicProperties.
   *
   * @param headers Map with parameters.
   * @return Equivalent of the input map but as AMQP.BasicProperties
   */
  public static AMQP.BasicProperties convertFrom(Map<String, Object> headers) {
    return new AMQP.BasicProperties.Builder().headers(headers).build();
  }

  /**
   * Reads RabbitMq connection parameters from properties file in classpath. File is searched inside
   * *.jar archive in its root folder.
   *
   * @param rabbitMqProperties *.properties file inside *.jar file which is in classpath.
   * @return record with connection parameters
   */
  public static RabbitMqConf readFromResources(String rabbitMqProperties) {
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
            properties.getProperty(
                "rabbitmq.user.password", new String(RabbitMqConf.DEFAULT_PASSWORD)));
    return new RabbitMqConf(host, port, user, password);
  }
}

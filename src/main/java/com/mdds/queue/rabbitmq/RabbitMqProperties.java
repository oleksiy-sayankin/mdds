/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

/**
 * Base container for RabbitMq properties.
 *
 * @param host RabbitMq host.
 * @param port RabbitMq port.
 * @param user RabbitMq user.
 * @param password RabbitMq user password.
 */
public record RabbitMqProperties(String host, int port, String user, String password) {
  public static final String DEFAULT_PROPERTIES_FILE = "rabbitmq.properties";
  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 5672;
  public static final String DEFAULT_USER = "guest";
  public static final char[] DEFAULT_PASSWORD = new char[] {'g', 'u', 'e', 's', 't'};
}

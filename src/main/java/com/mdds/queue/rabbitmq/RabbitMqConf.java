/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import static com.mdds.common.util.CommonHelper.readPropertiesOrEmpty;
import static com.mdds.common.util.ConfigResolution.resolveInt;
import static com.mdds.common.util.ConfigResolution.resolveString;

/**
 * Base container for RabbitMq properties.
 *
 * @param host RabbitMq host.
 * @param port RabbitMq port.
 * @param user RabbitMq user.
 * @param password RabbitMq user password.
 */
public record RabbitMqConf(String host, int port, String user, String password) {
  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 5672;
  public static final String DEFAULT_USER = "guest";
  public static final char[] DEFAULT_PASSWORD = new char[] {'g', 'u', 'e', 's', 't'};

  public static RabbitMqConf fromEnvOrProperties(String fileName) {
    var props = readPropertiesOrEmpty(fileName);
    var host = resolveString("rabbitmq.host", "RABBITMQ_HOST", props, DEFAULT_HOST);
    var port = resolveInt("rabbitmq.port", "RABBITMQ_PORT", props, DEFAULT_PORT);
    var user = resolveString("rabbitmq.user", "RABBITMQ_USER", props, DEFAULT_USER);
    var password =
        resolveString(
            "rabbitmq.password", "RABBITMQ_PASSWORD", props, new String(DEFAULT_PASSWORD));
    return new RabbitMqConf(host, port, user, password);
  }
}

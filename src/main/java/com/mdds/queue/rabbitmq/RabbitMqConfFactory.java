/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import static com.mdds.common.util.CommonHelper.readPropertiesOrEmpty;
import static com.mdds.common.util.ConfigResolution.resolveInt;
import static com.mdds.common.util.ConfigResolution.resolveString;

/** Factory for creating RabbitMq configurations. */
public final class RabbitMqConfFactory {
  private RabbitMqConfFactory() {}

  private static final String FILE_NAME = "rabbitmq.properties";

  public static RabbitMqConf fromEnvOrDefaultProperties() {
    var props = readPropertiesOrEmpty(FILE_NAME);
    var host = resolveString("rabbitmq.host", "RABBITMQ_HOST", props, "localhost");
    var port = resolveInt("rabbitmq.port", "RABBITMQ_PORT", props, 5672);
    var user = resolveString("rabbitmq.user", "RABBITMQ_USER", props, "guest");
    var password = resolveString("rabbitmq.password", "RABBITMQ_PASSWORD", props, "guest");
    return new RabbitMqConf(host, port, user, password);
  }
}

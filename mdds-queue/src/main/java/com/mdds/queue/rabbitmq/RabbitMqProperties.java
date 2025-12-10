/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Holder for RabbitMQ configuration properties. */
@ConfigurationProperties(prefix = "mdds.rabbitmq")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class RabbitMqProperties {
  private String host;
  private int port;
  private String user;
  private String password;
}

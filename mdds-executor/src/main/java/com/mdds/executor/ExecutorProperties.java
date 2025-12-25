/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Host, port and executor id properties of Executor server. */
@ConfigurationProperties(prefix = "mdds.executor")
@Getter
@Setter
public class ExecutorProperties {
  private String host;
  private int port;
  private String id;
  private String cancelQueueName;
}

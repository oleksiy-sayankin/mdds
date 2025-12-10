/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "mdds.common")
@Getter
@Setter
public class CommonProperties {
  private String taskQueueName = "default_mdds_task_queue";
  private String resultQueueName = "default_mdds_result_queue";
}

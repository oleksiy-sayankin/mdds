/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** Global application constants. */
@Getter
@AllArgsConstructor
public enum AppConstants {
  TASK_QUEUE_NAME("mdds.task.queue.name", "mdds_task_queue"),
  RESULT_QUEUE_NAME("mdds.result.queue.name", "mdds_result_queue");

  private final String key;
  private final String defaultValue;
}

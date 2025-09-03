/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

package com.mdds;

import dto.TaskDTO;

/** Common interface for Task Queue. */
public interface TaskQueue {
  void connect();

  void declareQueue(String queueName);

  void publish(TaskDTO task);

  void close();
}

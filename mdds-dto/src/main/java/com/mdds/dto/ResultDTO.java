/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

import jakarta.annotation.Nonnull;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Data Transfer Object for the result of the Executor's work. */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ResultDTO {
  @Nonnull String taskId;
  Instant dateTimeTaskCreated;
  Instant dateTimeTaskFinished;
  TaskStatus taskStatus;
  double[] solution;
  String errorMessage;
}

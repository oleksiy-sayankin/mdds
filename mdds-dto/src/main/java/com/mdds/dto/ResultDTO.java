/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

import com.mdds.grpc.solver.TaskStatus;
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
  Instant dateTimeTaskStarted;
  Instant dateTimeTaskEnded;
  TaskStatus taskStatus;
  String cancelQueueName;
  int progress;
  double[] solution;
  String errorMessage;

  public void setProgress(int progress) {
    if (progress < 0 || progress > 100) {
      throw new IllegalPercentValue("Progress must be between 0 and 100, but was " + progress);
    }
    this.progress = progress;
  }
}

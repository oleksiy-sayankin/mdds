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
  int percentDone;
  double[] solution;
  String errorMessage;

  public void setPercentDone(int percentDone) {
    if (percentDone < 0 || percentDone > 100) {
      throw new IllegalPercentValue(
          "Percent done must be between 0 and 100, but was " + percentDone);
    }
    this.percentDone = percentDone;
  }
}

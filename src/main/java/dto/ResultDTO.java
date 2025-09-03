/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package dto;

import java.time.Instant;
import lombok.Data;

/** Data Transfer Object for the result of the Executor's work. */
@Data
public class ResultDTO {
  String taskId;
  Instant dateTimeTaskCreated;
  Instant dateTimeTaskFinished;
  TaskStatus taskStatus;
  double[] solution;
  String errorMessage;
}

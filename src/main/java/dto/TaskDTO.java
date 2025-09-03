/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

package dto;

import java.time.Instant;
import lombok.Data;

/** Represents Data Transfer Object for the task for Executor. */
@Data
public class TaskDTO {
  private String id;
  private Instant dateTime;
  private double[][] matrix;
  private double[] rhs;
  private String SLAESolvingMethod;
}

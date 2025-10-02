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

/** Represents Data Transfer Object for the task for Executor. */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TaskDTO {
  private @Nonnull String id;
  private Instant dateTime;
  private double[][] matrix;
  private double[] rhs;
  private SlaeSolver slaeSolvingMethod;
}

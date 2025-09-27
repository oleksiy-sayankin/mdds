/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package dto;

import jakarta.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Data;

/** Represents response of the server that certain task (with id) is submitted for processing. */
@Data
@AllArgsConstructor
public class TaskIdResponseDTO {
  private @Nonnull String id;
}

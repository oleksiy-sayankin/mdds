/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

import jakarta.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Represents response of the server that certain job (with id) is submitted for processing. */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class JobIdResponseDTO {
  private @Nonnull String id;
}

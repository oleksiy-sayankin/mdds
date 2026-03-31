/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

import jakarta.validation.constraints.NotBlank;

public record JobUploadUrlRequestDTO(
    @NotBlank(message = "must not be null or blank.") String inputSlot) {}

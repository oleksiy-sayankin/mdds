/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto.rest.v1;

import jakarta.validation.constraints.NotBlank;

/** Response containing the identifier of a created or reused job. */
public record CreateJobResponseDTO(
    @NotBlank(message = "must not be null or blank.") String jobId) {}

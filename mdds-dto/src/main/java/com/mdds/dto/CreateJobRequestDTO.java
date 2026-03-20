/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

import jakarta.validation.constraints.NotBlank;

/** Request for job creation of certain type. */
public record CreateJobRequestDTO(
    @NotBlank(message = "must not be null or blank.") String jobType) {}

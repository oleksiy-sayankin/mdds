/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

import java.time.Instant;

/**
 * Response for get request for job status.
 *
 * @param jobId job identifier;
 * @param jobType job type from job profile;
 * @param status job status;
 * @param progress integer value from 0 to 100;
 * @param message nullable error message;
 * @param createdAt date/time of job creation;
 * @param submittedAt date/time of job submission;
 * @param startedAt date/time of job moving into in progress;
 * @param finishedAt date/time of job finishing.
 */
public record JobStatusResponseDTO(
    String jobId,
    String jobType,
    String status,
    int progress,
    String message,
    Instant createdAt,
    Instant submittedAt,
    Instant startedAt,
    Instant finishedAt) {}

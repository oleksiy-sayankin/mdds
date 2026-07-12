/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto.worker.v1;

import java.time.Instant;

/**
 * Status update published by Worker and consumed by StatusManagerService.
 *
 * @param jobId job identifier.
 * @param workerId worker identifier for IN_PROGRESS and cancellation targeting.
 * @param status new job status.
 * @param progress progress value from 0 to 100.
 * @param message nullable validation/error/status message.
 * @param eventTime time when Worker produced this status update.
 */
public record JobStatusUpdateDTO(
    String jobId,
    String workerId,
    String status,
    int progress,
    String message,
    Instant eventTime) {}

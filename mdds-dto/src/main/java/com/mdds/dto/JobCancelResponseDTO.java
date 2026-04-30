/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

/**
 * Response for job cancellation.
 *
 * @param jobId job identifier.
 * @param status job status.
 */
public record JobCancelResponseDTO(String jobId, String status) {}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto.rest.v1;

/**
 * Response for job submission.
 *
 * @param jobId job identifier.
 * @param status job status.
 */
public record SubmitJobResponseDTO(String jobId, String status) {}

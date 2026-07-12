/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto.rest.v1;

import java.time.Instant;

/**
 * Represents output message for job output request.
 *
 * @param jobId job identifier;
 * @param downloadUrl download URL;
 * @param expiresAt date and time of expiration.
 */
public record JobOutputResponseDTO(String jobId, String downloadUrl, Instant expiresAt) {}

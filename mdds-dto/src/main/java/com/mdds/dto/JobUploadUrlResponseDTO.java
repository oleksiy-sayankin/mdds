/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

import java.time.Instant;

/**
 * Response object for the issuing upload Url.
 *
 * @param jobId unique job identifier
 * @param uploadUrl generated upload url
 * @param expiresAt expiration date and time
 */
public record JobUploadUrlResponseDTO(String jobId, String uploadUrl, Instant expiresAt) {}

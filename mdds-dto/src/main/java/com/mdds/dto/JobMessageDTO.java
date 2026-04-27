/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

/**
 * Represents job for Worker.
 *
 * @param manifestObjectKey object key in s3 storage pointing to manifest.json
 */
public record JobMessageDTO(String manifestObjectKey) {}

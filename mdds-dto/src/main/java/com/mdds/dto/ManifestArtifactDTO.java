/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

/**
 * Represents file description in s3.
 *
 * @param objectKey objectKey to store data in s3
 * @param format format of file
 */
public record ManifestArtifactDTO(String objectKey, String format) {}

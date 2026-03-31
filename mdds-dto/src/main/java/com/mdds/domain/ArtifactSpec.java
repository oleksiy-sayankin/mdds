/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

/**
 * A job artifact that can be used as input entity or output entity.
 *
 * @param fileName real filename to store data
 */
public record ArtifactSpec(String fileName, ArtifactFormat format) {}

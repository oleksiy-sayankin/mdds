/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;

/** Represents job description. */
public record ManifestDTO(
    int manifestVersion,
    long userId,
    String jobId,
    String jobType,
    Map<String, ManifestArtifactDTO> inputs,
    Map<String, JsonNode> params,
    Map<String, ManifestArtifactDTO> outputs) {}

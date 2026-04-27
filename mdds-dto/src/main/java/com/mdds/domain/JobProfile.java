/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

import java.util.Map;

/**
 * Storage for job profile.
 *
 * @param enabled whether job profile is enabled.
 * @param inputArtifacts map with all input artifacts for the job.
 * @param outputArtifacts map with all output artifacts for the job.
 * @param paramSpecs map for job parameters.
 */
public record JobProfile(
    boolean enabled,
    Map<String, ArtifactSpec> inputArtifacts,
    Map<String, ArtifactSpec> outputArtifacts,
    Map<String, JobParamSpec> paramSpecs) {}

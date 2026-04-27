/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Stores all types of jobs available for execution.
 *
 * @param jobs list op job descriptions.
 */
@ConfigurationProperties(prefix = "mdds.job-profiles")
public record JobProfilesProperties(List<JobProfileConfig> jobs) {}

/**
 * @param type job type;
 * @param enabled indicates whether job creation for this type is enabled in the current API
 *     version;
 * @param inputSlots input data of the job;
 * @param params job input parameters;
 * @param outputSlots output data of the job;
 */
record JobProfileConfig(
    String type,
    boolean enabled,
    List<ArtifactConfig> inputSlots,
    List<JobParamConfig> params,
    List<ArtifactConfig> outputSlots) {}

/**
 * Description of job artifact.
 *
 * @param name name of the job artifact;
 * @param format format of the job artifact;
 * @param fileName name of the file where to store job artifact.
 */
record ArtifactConfig(String name, String format, String fileName) {}

/**
 * Description of job parameter.
 *
 * @param name name of the parameter;
 * @param type type of the parameter;
 * @param required true if required;
 * @param enumValues list of string values for enum.
 */
record JobParamConfig(String name, String type, boolean required, List<String> enumValues) {}

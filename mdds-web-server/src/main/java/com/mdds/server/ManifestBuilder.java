/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.mdds.domain.ArtifactSpec;
import com.mdds.domain.JobProfile;
import com.mdds.dto.ManifestArtifactDTO;
import com.mdds.dto.ManifestDTO;
import com.mdds.persistence.entity.JobParamEntity;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Helper class for manifest creation. */
public final class ManifestBuilder {
  private ManifestBuilder() {}

  public static ManifestDTO build(
      JobProfile jobProfile,
      int manifestVersion,
      long userId,
      String jobId,
      String jobType,
      List<JobParamEntity> params) {

    return new ManifestDTO(
        manifestVersion,
        userId,
        jobId,
        jobType,
        inputs(jobProfile, userId, jobId),
        params(params),
        outputs(jobProfile, userId, jobId));
  }

  private static Map<String, JsonNode> params(List<JobParamEntity> params) {
    var result = new LinkedHashMap<String, JsonNode>();
    for (JobParamEntity param : params) {
      var paramName = param.getId().getParamName();
      var paramValue = param.getParamValue();
      result.put(paramName, paramValue);
    }
    return result;
  }

  private static Map<String, ManifestArtifactDTO> inputs(
      JobProfile jobProfile, long userId, String jobId) {
    var inputs = new LinkedHashMap<String, ManifestArtifactDTO>();
    var inputArtifacts = jobProfile.inputArtifacts();
    for (Map.Entry<String, ArtifactSpec> artifact : inputArtifacts.entrySet()) {

      var inputSlot = artifact.getKey();
      var artifactSpec = artifact.getValue();
      var fileName = artifactSpec.fileName();
      var format = artifactSpec.format().getValue();
      inputs.put(
          inputSlot,
          new ManifestArtifactDTO(
              ObjectKeyBuilder.canonicalInputObjectKey(userId, jobId, fileName), format));
    }
    return inputs;
  }

  private static Map<String, ManifestArtifactDTO> outputs(
      JobProfile jobProfile, long userId, String jobId) {
    var outputs = new LinkedHashMap<String, ManifestArtifactDTO>();
    var outputArtifacts = jobProfile.outputArtifacts();
    for (Map.Entry<String, ArtifactSpec> artifact : outputArtifacts.entrySet()) {

      var outputSlot = artifact.getKey();
      var artifactSpec = artifact.getValue();
      var fileName = artifactSpec.fileName();
      var format = artifactSpec.format().getValue();
      outputs.put(
          outputSlot,
          new ManifestArtifactDTO(
              ObjectKeyBuilder.canonicalOutputObjectKey(userId, jobId, fileName), format));
    }
    return outputs;
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.domain.ArtifactFormat;
import com.mdds.domain.ArtifactSpec;
import com.mdds.domain.JobParamSpec;
import com.mdds.domain.JobProfile;
import com.mdds.domain.ParamType;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Helper class to convert job profile configuration created with Spring properties into domain
 * model.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class JobProfileMapper {

  static JobProfile toDomain(JobProfileConfig jobProfileConfig) {
    var inputArtifacts = new LinkedHashMap<String, ArtifactSpec>();
    var inputSlots = jobProfileConfig.inputSlots();
    Optional.ofNullable(inputSlots)
        .orElse(Collections.emptyList())
        .forEach(
            inputSlot -> {
              var inputSlotName = inputSlot.name();
              var fileName = inputSlot.fileName();
              var format = ArtifactFormat.from(inputSlot.format());
              inputArtifacts.put(inputSlotName, new ArtifactSpec(fileName, format));
            });

    var outputArtifacts = new LinkedHashMap<String, ArtifactSpec>();
    var outputSlots = jobProfileConfig.outputSlots();

    Optional.ofNullable(outputSlots)
        .orElse(Collections.emptyList())
        .forEach(
            outputSlot -> {
              var outputSlotName = outputSlot.name();
              var fileName = outputSlot.fileName();
              var format = ArtifactFormat.from(outputSlot.format());
              outputArtifacts.put(outputSlotName, new ArtifactSpec(fileName, format));
            });

    var paramSpecs = new LinkedHashMap<String, JobParamSpec>();
    var params = jobProfileConfig.params();

    Optional.ofNullable(params)
        .orElse(Collections.emptyList())
        .forEach(
            paramConfig -> {
              var paramName = paramConfig.name();
              var paramType = ParamType.from(paramConfig.type());
              var required = paramConfig.required();
              switch (paramType) {
                case STRING, NUMBER, BOOLEAN, MAP ->
                    paramSpecs.put(paramName, new JobParamSpec(paramType, required));
                case ENUM -> {
                  var enumValues =
                      new HashSet<String>(
                          Optional.ofNullable(paramConfig.enumValues())
                              .filter(list -> !list.isEmpty())
                              .orElseThrow(
                                  () ->
                                      new NoEnumValuesSpecifiedException(
                                          String.format(
                                              "No enum values specified for '%s'.", paramName))));
                  paramSpecs.put(paramName, new JobParamSpec(paramType, required, enumValues));
                }
              }
            });

    var enabled = jobProfileConfig.enabled();
    return new JobProfile(enabled, inputArtifacts, outputArtifacts, paramSpecs);
  }
}

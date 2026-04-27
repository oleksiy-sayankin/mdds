/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.server.JsonTypeFormatter.describeJsonType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.mdds.domain.JobProfile;
import com.mdds.domain.JobStatus;
import com.mdds.domain.ParamType;
import com.mdds.persistence.entity.JobEntity;
import com.mdds.persistence.entity.JobParamEntity;
import com.mdds.persistence.entity.JobParamId;
import com.mdds.server.jpa.JobParamsRepository;
import com.mdds.server.jpa.JobsRepository;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Merges the existing parameter set of a job with an incoming patch.
 *
 * <ul>
 *   <li>existing parameter + new value → update;
 *   <li>existing parameter + null → remove;
 *   <li>existing parameter omitted from the patch → keep unchanged;
 *   <li>new parameter with non-null value → add;
 *   <li>new parameter with null → do nothing.
 * </ul>
 */
@Service
@RequiredArgsConstructor
public class JobParamsService {

  private final JobParamsRepository jobParamsRepository;
  private final JobsRepository jobsRepository;
  private final JobProfileRegistry jobProfileRegistry;

  @Transactional
  public void mergeParams(
      long requestedUserId, String requestedJobId, Map<String, JsonNode> params) {

    // Here we lock by jobId to avoid race condition with job submit operation
    // so just one operation (job parameters patching or job submission)
    // will lock the job entity row and will be done.
    // Since jobId is private key in jobs table, it may seem that it is enough to lock
    // job entity only by jobId. Locking by pair jobId userId we check if a certain
    // job belongs to a user and if so, we lock the entity and throw exception otherwise.
    var existingJob =
        jobsRepository
            .lockByIdAndUserId(requestedJobId, requestedUserId)
            .orElseThrow(
                () ->
                    new JobDoesNotExistException(
                        String.format("Job with id '%s' does not exist.", requestedJobId)));

    var existingJobId = existingJob.getId();
    var existingJobType = existingJob.getJobType();

    if (!isInDraftState(existingJob)) {
      throw new JobIsNotDraftException(
          String.format(
              "Job '%s' is not in DRAFT state and no more job parameters can be patched.",
              existingJobId));
    }

    // early exit
    if (params.isEmpty()) {
      return;
    }

    var profile = jobProfileRegistry.forType(existingJobType);
    for (Map.Entry<String, JsonNode> paramEntry : params.entrySet()) {
      var paramName = paramEntry.getKey();
      if (isNullOrBlank(paramName)) {
        throw new JobParameterIsNullOrBlankException("Parameter name is blank or invalid.");
      }
      if (!validParamName(profile, paramName)) {
        throw new UnknownOrUnsupportedJobParameterException(
            String.format(
                "Unknown or unsupported parameter '%s' for the given job type: '%s'.",
                paramName, existingJobType));
      }
      var paramValue = paramEntry.getValue();
      if (isNullReference(paramValue)) {
        throw new JobParameterIsNullOrBlankException(
            String.format("Parameter '%s' has null value.", paramName));
      }
      var paramType = paramValue.getNodeType();
      if (!validParamType(profile, paramName, paramType)) {
        throw new InvalidJobParameterTypeException(
            String.format(
                "Parameter value '%s' for parameter '%s' has an invalid type '%s' for the given job"
                    + " type '%s'.",
                formatParamValue(paramValue),
                paramName,
                describeJsonType(paramType),
                existingJobType));
      }
      if (!validParamValue(profile, paramName, paramValue)) {
        throw new InvalidJobParameterValueException(
            String.format(
                "Invalid value '%s' of parameter '%s' for the given job type '%s'.",
                formatParamValue(paramValue), paramName, existingJobType));
      }
    }

    var existingParams = jobParamsRepository.findAllByIdJobId(existingJobId);

    var newParams = new ArrayList<JobParamEntity>();
    for (Map.Entry<String, JsonNode> paramEntry : params.entrySet()) {
      var paramName = paramEntry.getKey();
      var paramValue = paramEntry.getValue();
      var jobParamId = new JobParamId(existingJobId, paramName);
      var param = new JobParamEntity(jobParamId, paramValue);
      newParams.add(param);
    }

    var mergedParams = merge(existingParams, newParams);
    jobParamsRepository.deleteByIdJobId(existingJobId);
    jobParamsRepository.saveAll(mergedParams);
  }

  private static List<JobParamEntity> merge(
      List<JobParamEntity> existingParams, List<JobParamEntity> patchParams) {
    Map<JobParamId, JsonNode> merged = new LinkedHashMap<>();

    for (JobParamEntity existing : existingParams) {
      merged.put(existing.getId(), existing.getParamValue());
    }

    for (JobParamEntity patchParam : patchParams) {
      var id = patchParam.getId();
      var value = patchParam.getParamValue();

      if (value != null && value.isNull()) {
        merged.remove(id);
      } else {
        merged.put(id, value);
      }
    }

    var result = new ArrayList<JobParamEntity>();
    for (Map.Entry<JobParamId, JsonNode> entry : merged.entrySet()) {
      result.add(new JobParamEntity(entry.getKey(), entry.getValue()));
    }
    return result;
  }

  private static boolean validParamName(JobProfile jobProfile, String paramName) {
    return jobProfile.paramSpecs().containsKey(paramName);
  }

  private static boolean validParamValue(
      JobProfile jobProfile, String paramName, JsonNode paramValue) {
    if (paramValue == null) {
      return false;
    }
    // we allow Json null type as delete marker for parameter.
    if (paramValue.isNull()) {
      return true;
    }
    var spec = jobProfile.paramSpecs().get(paramName);
    var validType = spec.type();
    if (validType == ParamType.ENUM) {
      return spec.allowedValues().contains(paramValue.asText());
    }
    return true;
  }

  private static boolean validParamType(
      JobProfile jobProfile, String paramName, JsonNodeType paramValueType) {
    // we allow Json null type as delete marker for parameter.
    if (JsonNodeType.NULL.equals(paramValueType)) {
      return true;
    }

    var spec = jobProfile.paramSpecs().get(paramName);
    var validType = spec.type();

    if (validType == ParamType.ENUM) {
      return paramValueType == JsonNodeType.STRING;
    }

    return switch (validType) {
      case BOOLEAN -> paramValueType == JsonNodeType.BOOLEAN;
      case NUMBER -> paramValueType == JsonNodeType.NUMBER;
      case STRING -> paramValueType == JsonNodeType.STRING;
      case MAP -> paramValueType == JsonNodeType.OBJECT;
      default -> false;
    };
  }

  private static boolean isInDraftState(JobEntity jobEntity) {
    return JobStatus.DRAFT.equals(jobEntity.getStatus());
  }

  private static boolean isNullOrBlank(String value) {
    return value == null || value.isBlank();
  }

  private static boolean isNullReference(JsonNode value) {
    return value == null;
  }

  private static String formatParamValue(JsonNode value) {
    if (value == null || value.isNull()) {
      return "null";
    }

    if (value.isTextual() || value.isNumber() || value.isBoolean()) {
      return value.asText();
    }

    return value.toString();
  }
}

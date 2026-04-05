/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.server.JsonTypeFormatter.describeJsonType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.mdds.domain.JobProfile;
import com.mdds.domain.JobProfiles;
import com.mdds.domain.JobStatus;
import com.mdds.domain.ParamType;
import com.mdds.persistence.entity.JobEntity;
import com.mdds.persistence.entity.JobParamEntity;
import com.mdds.persistence.entity.JobParamId;
import com.mdds.server.jpa.JobParamsRepository;
import com.mdds.server.jpa.JobsRepository;
import java.util.ArrayList;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Replaces existing set of params to new one in a job. */
@Service
@RequiredArgsConstructor
public class JobParamsService {

  private final JobParamsRepository jobParamsRepository;
  private final JobsRepository jobsRepository;

  @Transactional
  public void replaceParams(
      long requestedUserId, String requestedJobId, Map<String, JsonNode> params) {

    // Here we lock by jobId to avoid race condition with job submit operation
    // so just one operation (job parameters replacement or job submission)
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
              "Job '%s' is not in DRAFT state and no more job parameters can be set.",
              existingJobId));
    }

    var profile = JobProfiles.forType(existingJobType);
    for (Map.Entry<String, JsonNode> paramEntry : params.entrySet()) {
      var paramName = paramEntry.getKey();
      if (isNullOrBlank(paramName)) {
        throw new JobParameterIsNullOrBlankException("Parameter name is blank or invalid.");
      }
      if (!validParamName(profile, paramName)) {
        throw new UnknownOrUnsupportedJobParameterException(
            String.format(
                "Unknown or unsupported parameter '%s' for the given job type: '%s'.",
                paramName, existingJobType.value()));
      }
      var paramValue = paramEntry.getValue();
      if (isNull(paramValue)) {
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
                existingJobType.value()));
      }
      if (!validParamValue(profile, paramName, paramValue)) {
        throw new InvalidJobParameterValueException(
            String.format(
                "Invalid value '%s' of parameter '%s' for the given job type '%s'.",
                formatParamValue(paramValue), paramName, existingJobType.value()));
      }
    }

    jobParamsRepository.deleteByIdJobId(existingJobId);

    var jobParams = new ArrayList<JobParamEntity>();
    for (Map.Entry<String, JsonNode> paramEntry : params.entrySet()) {
      var paramName = paramEntry.getKey();
      var paramValue = paramEntry.getValue();
      var jobParamId = new JobParamId(existingJobId, paramName);
      var param = new JobParamEntity(jobParamId, paramValue);
      jobParams.add(param);
    }
    jobParamsRepository.saveAll(jobParams);
  }

  private static boolean validParamName(JobProfile jobProfile, String paramName) {
    return jobProfile.paramSpecs().containsKey(paramName);
  }

  private static boolean validParamValue(
      JobProfile jobProfile, String paramName, JsonNode paramValue) {
    var spec = jobProfile.paramSpecs().get(paramName);
    var validType = spec.type();
    if (validType == ParamType.ENUM) {
      return spec.allowedValues().contains(paramValue.asText());
    }
    return true;
  }

  private static boolean validParamType(
      JobProfile jobProfile, String paramName, JsonNodeType paramValueType) {
    var spec = jobProfile.paramSpecs().get(paramName);
    var validType = spec.type();

    if (validType == ParamType.ENUM) {
      return paramValueType == JsonNodeType.STRING;
    }

    return switch (validType) {
      case BOOLEAN -> paramValueType == JsonNodeType.BOOLEAN;
      case NUMBER -> paramValueType == JsonNodeType.NUMBER;
      case STRING -> paramValueType == JsonNodeType.STRING;
      default -> false;
    };
  }

  private static boolean isInDraftState(JobEntity jobEntity) {
    return JobStatus.DRAFT.equals(jobEntity.getStatus());
  }

  private static boolean isNullOrBlank(String value) {
    return value == null || value.isBlank();
  }

  private static boolean isNull(JsonNode value) {
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

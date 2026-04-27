/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.domain.ArtifactSpec;
import com.mdds.domain.JobParamSpec;
import com.mdds.domain.JobStatus;
import com.mdds.dto.JobMessageDTO;
import com.mdds.persistence.entity.JobEntity;
import com.mdds.persistence.entity.JobParamEntity;
import com.mdds.queue.Message;
import com.mdds.queue.Queue;
import com.mdds.server.jpa.JobParamsRepository;
import com.mdds.server.jpa.JobsRepository;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Submits job: changes job state to submitted in RDBMS, creates job manifest and stores it in s3
 * storage, publishes job message to job queue.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class JobSubmissionService {
  private final JobParamsRepository jobParamsRepository;
  private final JobsRepository jobsRepository;
  private final ObjectStorageService objectStorageService;
  private final @Qualifier("jobQueue") Queue queue;
  private final JobProfileRegistry jobProfileRegistry;

  /**
   * Submits a draft job for execution.
   *
   * <p>This method:
   *
   * <ul>
   *   <li>locks the job record in the metadata database;
   *   <li>verifies structural readiness of the job;
   *   <li>creates and stores {@code manifest.json} in object storage;
   *   <li>publishes a submitted job message to the execution queue;
   *   <li>updates the job status to {@code SUBMITTED} in the database.
   * </ul>
   *
   * <p><strong>Consistency note:</strong> this operation is transactional only with respect to the
   * relational database. Object storage and queue publication are external side effects and are not
   * part of the same atomic transaction. Therefore, partial success is possible, for example:
   *
   * <ul>
   *   <li>the manifest is written but the queue publish fails;
   *   <li>the message is published but the database update fails.
   * </ul>
   *
   * <p>A more reliable design would use an outbox / reliable publish pattern.
   *
   * @param requestedUserId user id from client request.
   * @param requestedJobId job id from client request.
   */
  @Transactional
  public void submit(long requestedUserId, String requestedJobId) {

    var existingJob =
        jobsRepository
            .lockByIdAndUserId(requestedJobId, requestedUserId)
            .orElseThrow(
                () ->
                    new JobDoesNotExistException(
                        String.format("Job with id '%s' does not exist.", requestedJobId)));

    var existingJobId = existingJob.getId();
    var existingJobType = existingJob.getJobType();
    var existingUserId = existingJob.getUserId();

    if (!isInDraftState(existingJob)) {
      throw new JobIsNotDraftException(
          String.format(
              "Job '%s' is not in DRAFT state and submission is not allowed.", existingJobId));
    }

    var existingParams = jobParamsRepository.findAllByIdJobId(existingJobId);
    var profile = jobProfileRegistry.forType(existingJobType);
    var specifiedParams = profile.paramSpecs();
    for (Map.Entry<String, JobParamSpec> specifiedParam : specifiedParams.entrySet()) {
      if (isRequired(specifiedParam)) {
        var specifiedParamName = specifiedParam.getKey();
        if (!contains(existingParams, specifiedParamName)) {
          throw new RequiredParameterIsAbsentException(
              String.format("Required parameter '%s' is absent.", specifiedParamName));
        }
      }
    }

    var specifiedInputArtifacts = profile.inputArtifacts();
    for (Map.Entry<String, ArtifactSpec> specifiedInputArtifact :
        specifiedInputArtifacts.entrySet()) {
      var fileName = specifiedInputArtifact.getValue().fileName();
      var objectKey =
          ObjectKeyBuilder.canonicalInputObjectKey(existingUserId, existingJobId, fileName);
      if (!objectStorageService.exists(objectKey)) {
        throw new RequiredInputArtifactIsAbsentException(
            String.format("Required input artifact '%s' is absent in object storage.", fileName));
      }
    }

    var manifest =
        ManifestBuilder.build(
            profile, 1, existingUserId, existingJobId, existingJobType, existingParams);
    var manifestObjectKey = ObjectKeyBuilder.manifestObjectKey(existingUserId, existingJobId);
    objectStorageService.putManifest(manifestObjectKey, manifest);

    var now = Instant.now();
    var queueName = "queue-" + existingJobType;

    queue.publish(
        queueName,
        new Message<>(new JobMessageDTO(manifestObjectKey), Collections.emptyMap(), now));
    log.info("Published job to queue '{}' = {}", queueName, queue);

    existingJob.setSubmittedAt(now);
    existingJob.setStatus(JobStatus.SUBMITTED);
    jobsRepository.save(existingJob);
  }

  private static boolean contains(List<JobParamEntity> params, String specifiedParamName) {
    for (JobParamEntity param : params) {
      if (param.getId().getParamName().equals(specifiedParamName)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isRequired(Map.Entry<String, JobParamSpec> param) {
    return param.getValue().required();
  }

  private static boolean isInDraftState(JobEntity jobEntity) {
    return JobStatus.DRAFT.equals(jobEntity.getStatus());
  }
}

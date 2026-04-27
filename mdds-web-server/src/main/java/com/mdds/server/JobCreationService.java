/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.domain.JobStatus;
import com.mdds.persistence.entity.JobEntity;
import com.mdds.server.jpa.JobsRepository;
import com.mdds.server.jpa.UsersRepository;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * This service creates new Job items if it not exists for pair (userId, uploadSessionId) or returns
 * jobId if job has been already created for certain pair (userId, uploadSessionId).
 */
@Service
@RequiredArgsConstructor
public class JobCreationService {
  private final JobsRepository jobsRepository;
  private final UsersRepository usersRepository;
  private final JobProfileRegistry jobProfileRegistry;

  /**
   * Looks up for pair (<i>userId</i>, <i>uploadSessionId</i>). If exists, it returns record
   * (<i>jobId</i>, <i>false</i>). Here <i>false</i> is the answer to the question: was Job created?
   * Otherwise, it creates new job, stores it in metadata Db and returns (<i>jobId</i>,
   * <i>true</i>).
   *
   * @param userId user identifier in DB.
   * @param uploadSessionId upload session from client.
   * @param jobType job type.
   * @return record (<i>jobId</i>, <i>false/true</i>) depending on whether <i>jobId</i> was created
   *     or existed before.
   */
  @Transactional
  public JobCreationResult createOrReuseDraftJob(
      long userId, String uploadSessionId, String jobType) {
    if (!isValid(jobType) || !enabled(jobType)) {
      throw new UnknownOrUnsupportedJobTypeException(
          String.format("Unknown or unsupported job type: %s.", jobType));
    }
    if (uploadSessionId == null || uploadSessionId.isBlank()) {
      throw new UploadSessionIdIsNullOrBlankException("Upload session id is null or blank.");
    }
    // intentionally correct but not maximally scalable
    usersRepository
        .lockById(userId)
        .orElseThrow(
            () -> new IllegalStateException("User disappeared during job creation: " + userId));
    var existing = jobsRepository.findByUserIdAndUploadSessionId(userId, uploadSessionId);
    if (existing.isPresent()) {
      var existingJob = existing.get();
      var existingJobType = existingJob.getJobType();

      if (existingJob.getStatus() != JobStatus.DRAFT) {
        throw new JobIsNotDraftException(
            "Upload session id '"
                + uploadSessionId
                + "' is already bound to job '"
                + existingJob.getId()
                + "' with status '"
                + existingJob.getStatus()
                + "'. A new upload session id is required.");
      }

      if (!existingJobType.equals(jobType)) {
        throw new JobTypeConflictException(
            "A draft job already exists for upload session id"
                + " '"
                + uploadSessionId
                + "' with job type '"
                + existingJobType
                + "', which does not match requested job type '"
                + jobType
                + "'.");
      }
      return new JobCreationResult(existingJob.getId(), false);
    } else {
      var jobId = UUID.randomUUID().toString();
      var job = new JobEntity();
      job.setId(jobId);
      job.setUserId(userId);
      job.setUploadSessionId(uploadSessionId);
      job.setJobType(jobType);
      job.setStatus(JobStatus.DRAFT);
      job.setProgress(0);
      job.setCreatedAt(Instant.now());
      jobsRepository.save(job);
      return new JobCreationResult(jobId, true);
    }
  }

  private boolean isValid(String jobType) {
    return jobProfileRegistry.jobTypes().contains(jobType);
  }

  private boolean enabled(String jobType) {
    return jobProfileRegistry.forType(jobType).enabled();
  }
}

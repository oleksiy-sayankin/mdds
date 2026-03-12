/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.dto.JobStatus;
import com.mdds.dto.Jobs;
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
public class JobInitService {
  private final JobsRepository jobsRepository;
  private final UsersRepository usersRepository;

  @Transactional
  public String findOrCreateJobId(long userId, String uploadSessionId) {
    usersRepository.lockById(userId);
    return jobsRepository
        .findByUserIdAndUploadSessionId(userId, uploadSessionId)
        .map(Jobs::getId)
        .orElseGet(
            () -> {
              var jobId = UUID.randomUUID().toString();
              var job = new Jobs();
              job.setId(jobId);
              job.setUserId(userId);
              job.setUploadSessionId(uploadSessionId);
              job.setStatus(JobStatus.NEW);
              job.setProgress(0);
              job.setCreatedAt(Instant.now());
              jobsRepository.save(job);
              return jobId;
            });
  }
}

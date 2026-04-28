/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.server.jpa.JobsRepository;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Returns job status. */
@Service
@RequiredArgsConstructor
public class JobStatusService {
  private final JobsRepository jobsRepository;

  @Transactional(readOnly = true)
  public JobStatusResult status(long requestedUserId, String requestedJobId) {
    var existingJob =
        jobsRepository
            .findByIdAndUserId(requestedJobId, requestedUserId)
            .orElseThrow(
                () ->
                    new JobDoesNotExistException(
                        String.format("Job with id '%s' does not exist.", requestedJobId)));

    return new JobStatusResult(
        existingJob.getId(),
        existingJob.getJobType(),
        existingJob.getStatus().toString(),
        existingJob.getProgress(),
        existingJob.getMessage(),
        existingJob.getCreatedAt(),
        existingJob.getSubmittedAt(),
        existingJob.getStartedAt(),
        existingJob.getFinishedAt());
  }

  public record JobStatusResult(
      String jobId,
      String jobType,
      String status,
      int progress,
      String message,
      Instant createdAt,
      Instant submittedAt,
      Instant startedAt,
      Instant finishedAt) {}
}

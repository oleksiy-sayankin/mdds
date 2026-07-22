/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.domain.JobStatus.CANCEL_REQUESTED;
import static com.mdds.domain.JobStatus.DONE;
import static com.mdds.domain.JobStatus.DRAFT;
import static com.mdds.domain.JobStatus.IN_PROGRESS;
import static com.mdds.domain.JobStatus.SUBMITTED;

import com.mdds.domain.JobStatus;
import com.mdds.dto.worker.v1.JobStatusUpdateDTO;
import com.mdds.server.jpa.JobsRepository;
import java.time.Instant;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class JobStatusUpdateService {
  private final JobsRepository jobsRepository;

  @Transactional
  public JobStatusUpdateResult apply(JobStatusUpdateDTO update) {
    if (update == null) {
      throw new IllegalJobStatusUpdateException("Status update must not be null.");
    }
    validateJobId(update.jobId());
    var newStatus = JobStatus.from(update.status());
    var eventTime = update.eventTime();
    validateEventTime(eventTime);

    var job =
        jobsRepository
            .lockById(update.jobId())
            .orElseThrow(
                () ->
                    new JobDoesNotExistException(
                        "Job with id '" + update.jobId() + "' does not exist."));

    var existingStatus = job.getStatus();
    var existingWorkerId = job.getWorkerId();
    var newWorkerId = update.workerId();
    var newProgress = update.progress();

    validateProgress(newProgress, newStatus);
    validateWorkerStatusUpdate(newStatus, newWorkerId);
    validateStatusTransition(existingStatus, newStatus);
    validateWorkerOwnership(existingWorkerId, newWorkerId, job.getId());

    job.setStatus(newStatus);
    job.setProgress(newProgress);
    job.setMessage(update.message());

    if (job.getWorkerId() == null || job.getWorkerId().isBlank()) {
      job.setWorkerId(newWorkerId);
    }

    if (newStatus == IN_PROGRESS && job.getStartedAt() == null) {
      job.setStartedAt(eventTime);
    }

    if (newStatus.isTerminal()) {
      job.setFinishedAt(eventTime);
    }

    jobsRepository.save(job);
    return new JobStatusUpdateResult(job.getId(), job.getUserId(), job.getStatus());
  }

  public record JobStatusUpdateResult(String jobId, long userId, JobStatus status) {}

  private static void validateWorkerStatusUpdate(JobStatus newStatus, String workerId) {
    if (Set.of(DRAFT, SUBMITTED, CANCEL_REQUESTED).contains(newStatus)) {
      throw new IllegalJobStatusUpdateException(
          "Worker is not allowed to publish status '" + newStatus.getCode() + "'.");
    }

    if (workerId == null || workerId.isBlank()) {
      throw new IllegalJobStatusUpdateException(
          "workerId is required for '" + newStatus.getCode() + "' status update.");
    }
  }

  private static void validateJobId(String jobId) {
    if (jobId == null || jobId.isBlank()) {
      throw new IllegalJobStatusUpdateException("jobId is required.");
    }
  }

  private static void validateWorkerOwnership(
      String existingWorkerId, String newWorkerId, String jobId) {
    if (existingWorkerId == null || existingWorkerId.isBlank()) {
      return;
    }

    if (!existingWorkerId.equals(newWorkerId)) {
      throw new IllegalJobStatusUpdateException(
          "Job '" + jobId + "' is already owned by another worker.");
    }
  }

  private static void validateProgress(int progress, JobStatus newStatus) {
    if (progress < 0 || progress > 100) {
      throw new IllegalJobStatusUpdateException("Progress must be between 0 and 100.");
    }

    if (newStatus == DONE && progress != 100) {
      throw new IllegalJobStatusUpdateException("DONE status requires progress 100.");
    }
  }

  private static void validateEventTime(Instant eventTime) {
    if (eventTime == null) {
      throw new IllegalEventTimeStatusUpdateException("Event time is required.");
    }
  }

  private static void validateStatusTransition(JobStatus existingStatus, JobStatus newStatus) {
    if (!existingStatus.canSwitchTo(newStatus)) {
      throw new IllegalJobStatusUpdateException(
          "Illegal status transition from '"
              + existingStatus.getCode()
              + "' to '"
              + newStatus.getCode()
              + "'.");
    }
  }
}

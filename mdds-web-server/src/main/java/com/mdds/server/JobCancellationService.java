/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.domain.JobStatus;
import com.mdds.dto.CancelJobDTO;
import com.mdds.persistence.entity.JobEntity;
import com.mdds.queue.CancelBus;
import com.mdds.queue.Message;
import com.mdds.server.jpa.JobsRepository;
import java.time.Clock;
import java.util.Collections;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Requests job cancellation. */
@Slf4j
@Service
@RequiredArgsConstructor
public class JobCancellationService {
  private final JobsRepository jobsRepository;
  private final CancelBus cancelBus;
  private final Clock clock;

  /**
   * Creates job cancel request.
   *
   * <p>This method:
   *
   * <ul>
   *   <li>locks the job record in the metadata database;
   *   <li>verifies that job can be cancelled;
   *   <li>publishes a cancel request message to the cancel queue;
   *   <li>updates the job status to {@code CANCEL_REQUESTED} in the database.
   * </ul>
   *
   * <p><strong>Consistency note:</strong> this operation is transactional only with respect to the
   * relational database. Queue publications are external side effects and are not part of the same
   * atomic transaction. Therefore, partial success is possible, for example:
   *
   * <ul>
   *   <li>the cancel message is published but the database update or transaction commit fails;
   * </ul>
   *
   * <p>A more reliable design would use an outbox / reliable publish pattern.
   *
   * @param requestedUserId user id from client request.
   * @param requestedJobId job id from client request.
   */
  @Transactional
  public void cancel(long requestedUserId, String requestedJobId) {
    var existingJob =
        jobsRepository
            .lockByIdAndUserId(requestedJobId, requestedUserId)
            .orElseThrow(
                () ->
                    new JobDoesNotExistException(
                        String.format("Job with id '%s' does not exist.", requestedJobId)));

    var existingJobId = existingJob.getId();
    var existingJobStatus = existingJob.getStatus();

    if (existingJobStatus == JobStatus.CANCEL_REQUESTED) {
      log.warn("Job '{}' is already cancellation requested.", existingJobId);
      return;
    }

    if (isInTerminalState(existingJob)) {
      throw new JobIsInTerminalStateException(
          String.format(
              "Job '%s' is in terminal state '%s' and cancellation is not allowed.",
              existingJobId, existingJobStatus.getCode()));
    }

    if (!isInProgress(existingJob)) {
      throw new JobIsNotRunningException(
          String.format(
              "Job '%s' is in state '%s' and cancellation is supported only for 'IN_PROGRESS'"
                  + " jobs.",
              existingJobId, existingJobStatus.getCode()));
    }

    var existingJobWorkerId = existingJob.getWorkerId();
    if (existingJobWorkerId == null || existingJobWorkerId.isBlank()) {
      log.error(
          "Job '{}' is in state '{}' but workerId is blank or null.",
          existingJobId,
          existingJobStatus.getCode());
      throw new JobHasNoWorkerAssignedException(
          String.format(
              "Job '%s' is in state '%s' but workerId is not assigned.",
              existingJobId, existingJobStatus.getCode()));
    }

    cancelBus.sendCancel(
        existingJobWorkerId,
        new Message<>(new CancelJobDTO(existingJobId), Collections.emptyMap(), clock.instant()));

    log.info(
        "Published cancel request for job '{}' and worker '{}'",
        existingJobId,
        existingJobWorkerId);

    existingJob.setStatus(JobStatus.CANCEL_REQUESTED);
    jobsRepository.save(existingJob);
  }

  private static boolean isInProgress(JobEntity jobEntity) {
    return JobStatus.IN_PROGRESS.equals(jobEntity.getStatus());
  }

  private static boolean isInTerminalState(JobEntity jobEntity) {
    return Set.of(JobStatus.DONE, JobStatus.CANCELLED, JobStatus.ERROR, JobStatus.VALIDATION_FAILED)
        .contains(jobEntity.getStatus());
  }
}

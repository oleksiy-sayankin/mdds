/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.domain.JobStatus.CANCELLED;
import static com.mdds.domain.JobStatus.CANCEL_REQUESTED;
import static com.mdds.domain.JobStatus.DONE;
import static com.mdds.domain.JobStatus.DRAFT;
import static com.mdds.domain.JobStatus.ERROR;
import static com.mdds.domain.JobStatus.IN_PROGRESS;
import static com.mdds.domain.JobStatus.SUBMITTED;
import static com.mdds.domain.JobStatus.VALIDATION_FAILED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.mdds.domain.JobStatus;
import com.mdds.domain.UnknownJobStatusException;
import com.mdds.dto.JobStatusUpdateDTO;
import com.mdds.server.jpa.JobsRepository;
import com.mdds.server.support.JobTestFixture;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
@Import(JobTestFixture.class)
class TestJobStatusUpdateServiceIntegration {

  private static final Instant BASE_EVENT_TIME = Instant.parse("2026-01-01T00:00:00Z");

  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17")
          .withDatabaseName("mdds")
          .withUsername("mdds")
          .withPassword("mdds123");

  @Autowired private JobStatusUpdateService jobStatusUpdateService;
  @Autowired private JobCreationService jobCreationService;
  @Autowired private UserLookupService userLookupService;
  @Autowired private JobTestFixture jobFixture;
  @Autowired private JobsRepository jobsRepository;

  @DynamicPropertySource
  static void registerProps(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
  }

  private static final String GUEST = "guest";
  private static final String ADMIN = "admin";

  private static Stream<String> userLoginValues() {
    return Stream.of(GUEST, ADMIN);
  }

  @ParameterizedTest
  @MethodSource("userLoginValues")
  void testApply(String login) {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(login);
    var response = createOrReuseDraftJob(userId, sessionId, jobType);
    var jobId = response.jobId();
    jobFixture.forceStatus(jobId, SUBMITTED);
    var workerId = newWorkerId();
    var eventTime = BASE_EVENT_TIME;
    var progress = 1;
    var status = IN_PROGRESS;
    var update =
        new JobStatusUpdateDTO(
            jobId, workerId, status.getCode(), progress, "Worker started processing", eventTime);
    var result = jobStatusUpdateService.apply(update);
    assertThat(result.jobId()).isEqualTo(jobId);
    assertThat(result.status()).isEqualTo(status);
    assertThat(result.userId()).isEqualTo(userId);
    var job = jobsRepository.findById(jobId).orElseThrow();
    assertThat(job.getProgress()).isEqualTo(progress);
    var before = eventTime.minusMillis(10);
    var after = eventTime.plusMillis(10);
    assertThat(job.getStartedAt()).isBetween(before, after);
    assertThat(job.getStatus()).isEqualTo(IN_PROGRESS);
    assertThat(job.getWorkerId()).isEqualTo(workerId);
    assertThat(job.getMessage()).isEqualTo("Worker started processing");
    assertThat(job.getFinishedAt()).isNull();
  }

  @ParameterizedTest
  @MethodSource("userLoginValues")
  void testApplyFromProgressToDone(String login) {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(login);
    var response = createOrReuseDraftJob(userId, sessionId, jobType);
    var jobId = response.jobId();
    jobFixture.forceStatus(jobId, SUBMITTED);
    var workerId = newWorkerId();
    var startedTime = BASE_EVENT_TIME;

    var progress = 10;
    var status = IN_PROGRESS;
    var firstUpdate =
        new JobStatusUpdateDTO(
            jobId, workerId, status.getCode(), progress, "Worker started processing", startedTime);
    jobStatusUpdateService.apply(firstUpdate);

    status = DONE;
    progress = 100;
    var finishedTime = BASE_EVENT_TIME;
    var secondUpdate =
        new JobStatusUpdateDTO(
            jobId,
            workerId,
            status.getCode(),
            progress,
            "Worker finished processing",
            finishedTime);
    var result = jobStatusUpdateService.apply(secondUpdate);

    assertThat(result.jobId()).isEqualTo(jobId);
    assertThat(result.status()).isEqualTo(status);
    assertThat(result.userId()).isEqualTo(userId);
    var job = jobsRepository.findById(jobId).orElseThrow();
    assertThat(job.getProgress()).isEqualTo(progress);
    var beforeStarted = startedTime.minusMillis(10);
    var afterStarted = startedTime.plusMillis(10);
    assertThat(job.getStartedAt()).isBetween(beforeStarted, afterStarted);
    assertThat(job.getStatus()).isEqualTo(DONE);
    assertThat(job.getWorkerId()).isEqualTo(workerId);
    assertThat(job.getMessage()).isEqualTo("Worker finished processing");
    var beforeFinished = finishedTime.minusMillis(10);
    var afterFinished = finishedTime.plusMillis(10);
    assertThat(job.getFinishedAt()).isBetween(beforeFinished, afterFinished);
  }

  @ParameterizedTest
  @MethodSource("userLoginValues")
  void testApplyFromCancelRequestedToCancelled(String login) {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(login);
    var response = createOrReuseDraftJob(userId, sessionId, jobType);
    var jobId = response.jobId();
    jobFixture.forceStatus(jobId, SUBMITTED);
    var workerId = newWorkerId();
    var startedTime = BASE_EVENT_TIME;

    var progress = 10;
    var status = IN_PROGRESS;
    var firstUpdate =
        new JobStatusUpdateDTO(
            jobId, workerId, status.getCode(), progress, "Worker started processing", startedTime);
    jobStatusUpdateService.apply(firstUpdate);

    jobFixture.forceStatus(jobId, CANCEL_REQUESTED);

    status = CANCELLED;
    progress = 25;
    var finishedTime = BASE_EVENT_TIME;
    var secondUpdate =
        new JobStatusUpdateDTO(
            jobId,
            workerId,
            status.getCode(),
            progress,
            "Worker cancelled processing",
            finishedTime);
    var result = jobStatusUpdateService.apply(secondUpdate);

    assertThat(result.jobId()).isEqualTo(jobId);
    assertThat(result.status()).isEqualTo(status);
    assertThat(result.userId()).isEqualTo(userId);
    var job = jobsRepository.findById(jobId).orElseThrow();
    assertThat(job.getProgress()).isEqualTo(progress);
    var beforeStarted = startedTime.minusMillis(10);
    var afterStarted = startedTime.plusMillis(10);
    assertThat(job.getStartedAt()).isBetween(beforeStarted, afterStarted);
    assertThat(job.getStatus()).isEqualTo(CANCELLED);
    assertThat(job.getWorkerId()).isEqualTo(workerId);
    assertThat(job.getMessage()).isEqualTo("Worker cancelled processing");
    var beforeFinished = finishedTime.minusMillis(10);
    var afterFinished = finishedTime.plusMillis(10);
    assertThat(job.getFinishedAt()).isBetween(beforeFinished, afterFinished);
  }

  @ParameterizedTest
  @MethodSource("userLoginValues")
  void testApplyFromInProgressToError(String login) {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(login);
    var response = createOrReuseDraftJob(userId, sessionId, jobType);
    var jobId = response.jobId();
    jobFixture.forceStatus(jobId, SUBMITTED);
    var workerId = newWorkerId();
    var startedTime = BASE_EVENT_TIME;

    var progress = 10;
    var status = IN_PROGRESS;
    var firstUpdate =
        new JobStatusUpdateDTO(
            jobId, workerId, status.getCode(), progress, "Worker started processing", startedTime);
    jobStatusUpdateService.apply(firstUpdate);

    status = ERROR;
    progress = 25;
    var finishedTime = BASE_EVENT_TIME;
    var secondUpdate =
        new JobStatusUpdateDTO(
            jobId,
            workerId,
            status.getCode(),
            progress,
            "Worker finished processing with error",
            finishedTime);
    var result = jobStatusUpdateService.apply(secondUpdate);

    assertThat(result.jobId()).isEqualTo(jobId);
    assertThat(result.status()).isEqualTo(status);
    assertThat(result.userId()).isEqualTo(userId);
    var job = jobsRepository.findById(jobId).orElseThrow();
    assertThat(job.getProgress()).isEqualTo(progress);
    var beforeStarted = startedTime.minusMillis(10);
    var afterStarted = startedTime.plusMillis(10);
    assertThat(job.getStartedAt()).isBetween(beforeStarted, afterStarted);
    assertThat(job.getStatus()).isEqualTo(ERROR);
    assertThat(job.getWorkerId()).isEqualTo(workerId);
    assertThat(job.getMessage()).isEqualTo("Worker finished processing with error");
    var beforeFinished = finishedTime.minusMillis(10);
    var afterFinished = finishedTime.plusMillis(10);
    assertThat(job.getFinishedAt()).isBetween(beforeFinished, afterFinished);
  }

  @Test
  void testApplyProgressToProgressUpdateDoesNotRewriteStartedAt() {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var jobId = createOrReuseDraftJob(userId, sessionId, jobType).jobId();
    jobFixture.forceStatus(jobId, SUBMITTED);

    var workerId = newWorkerId();
    var firstEventTime = BASE_EVENT_TIME;

    jobStatusUpdateService.apply(
        new JobStatusUpdateDTO(
            jobId,
            workerId,
            IN_PROGRESS.getCode(),
            1,
            "Worker started processing",
            firstEventTime));

    var secondEventTime = firstEventTime.plusSeconds(10);

    jobStatusUpdateService.apply(
        new JobStatusUpdateDTO(
            jobId, workerId, IN_PROGRESS.getCode(), 40, "Worker is processing", secondEventTime));

    var job = jobsRepository.findById(jobId).orElseThrow();
    assertThat(job.getStatus()).isEqualTo(IN_PROGRESS);
    assertThat(job.getWorkerId()).isEqualTo(workerId);
    assertThat(job.getProgress()).isEqualTo(40);
    var after = firstEventTime.plusMillis(10);
    var before = firstEventTime.minusMillis(10);
    assertThat(job.getStartedAt()).isBetween(before, after);
    assertThat(job.getFinishedAt()).isNull();
    assertThat(job.getMessage()).isEqualTo("Worker is processing");
  }

  @Test
  void testApplyJobDoesNotExist() {
    var jobId = "wrong_job_id";
    var workerId = newWorkerId();
    var eventTime = BASE_EVENT_TIME;
    var progress = 1;
    var update =
        new JobStatusUpdateDTO(
            jobId,
            workerId,
            IN_PROGRESS.getCode(),
            progress,
            "Worker started processing",
            eventTime);

    assertThatExceptionOfType(JobDoesNotExistException.class)
        .isThrownBy(() -> jobStatusUpdateService.apply(update))
        .withMessage("Job with id '" + jobId + "' does not exist.");
  }

  private static Stream<String> jobInvalidJobIdValues() {
    return Stream.of(null, "", " ");
  }

  @ParameterizedTest
  @MethodSource("jobInvalidJobIdValues")
  void testApplyJobIdIsNullOrBlank(String jobId) {
    var workerId = newWorkerId();
    var eventTime = BASE_EVENT_TIME;
    var progress = 1;
    var update =
        new JobStatusUpdateDTO(
            jobId,
            workerId,
            IN_PROGRESS.getCode(),
            progress,
            "Worker started processing",
            eventTime);

    assertThatExceptionOfType(IllegalJobStatusUpdateException.class)
        .isThrownBy(() -> jobStatusUpdateService.apply(update))
        .withMessage("jobId is required.");
  }

  @Test
  void testApplyNullEventTime() {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var response = createOrReuseDraftJob(userId, sessionId, jobType);
    var jobId = response.jobId();
    jobFixture.forceStatus(jobId, JobStatus.SUBMITTED);
    var workerId = newWorkerId();
    var progress = 1;
    var update =
        new JobStatusUpdateDTO(
            jobId, workerId, IN_PROGRESS.getCode(), progress, "Worker started processing", null);

    assertThatExceptionOfType(IllegalEventTimeStatusUpdateException.class)
        .isThrownBy(() -> jobStatusUpdateService.apply(update))
        .withMessage("Event time is required.");
  }

  private static Stream<Integer> jobInvalidProgressValues() {
    return Stream.of(-1, 101);
  }

  @ParameterizedTest
  @MethodSource("jobInvalidProgressValues")
  void testApplyInvalidProgress(int progress) {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var response = createOrReuseDraftJob(userId, sessionId, jobType);
    var jobId = response.jobId();
    jobFixture.forceStatus(jobId, JobStatus.SUBMITTED);
    var workerId = newWorkerId();
    var eventTime = BASE_EVENT_TIME;
    var update =
        new JobStatusUpdateDTO(
            jobId,
            workerId,
            IN_PROGRESS.getCode(),
            progress,
            "Worker started processing",
            eventTime);

    assertThatExceptionOfType(IllegalJobStatusUpdateException.class)
        .isThrownBy(() -> jobStatusUpdateService.apply(update))
        .withMessage("Progress must be between 0 and 100.");
  }

  private static Stream<Integer> jobValidProgressValues() {
    return Stream.of(0, 1, 10, 99);
  }

  @ParameterizedTest
  @MethodSource("jobValidProgressValues")
  void testApplyInvalidProgressWhenDone(int progress) {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var response = createOrReuseDraftJob(userId, sessionId, jobType);
    var jobId = response.jobId();
    var workerId = newWorkerId();
    jobFixture.forceStatus(jobId, IN_PROGRESS);
    jobFixture.forceWorkerId(jobId, workerId);
    var eventTime = BASE_EVENT_TIME;
    var update =
        new JobStatusUpdateDTO(
            jobId, workerId, DONE.getCode(), progress, "Worker finished processing", eventTime);

    assertThatExceptionOfType(IllegalJobStatusUpdateException.class)
        .isThrownBy(() -> jobStatusUpdateService.apply(update))
        .withMessage("DONE status requires progress 100.");
  }

  private static Stream<JobStatus> jobInvalidStatusValues() {
    return Stream.of(DRAFT, SUBMITTED, CANCEL_REQUESTED);
  }

  @Test
  void testApplyUnknownStatus() {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var response = createOrReuseDraftJob(userId, sessionId, jobType);
    var jobId = response.jobId();
    jobFixture.forceStatus(jobId, SUBMITTED);
    var workerId = newWorkerId();
    var update =
        new JobStatusUpdateDTO(
            jobId, workerId, "UNKNOWN_STATUS", 10, "Bad status", BASE_EVENT_TIME);

    assertThatExceptionOfType(UnknownJobStatusException.class)
        .isThrownBy(() -> jobStatusUpdateService.apply(update))
        .withMessage("Unknown or unsupported job status: 'UNKNOWN_STATUS'.");
  }

  @ParameterizedTest
  @MethodSource("jobInvalidStatusValues")
  void testApplyInvalidJobStatus(JobStatus status) {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var response = createOrReuseDraftJob(userId, sessionId, jobType);
    var jobId = response.jobId();
    jobFixture.forceStatus(jobId, JobStatus.SUBMITTED);
    var workerId = newWorkerId();
    var eventTime = BASE_EVENT_TIME;
    var progress = 10;
    var update =
        new JobStatusUpdateDTO(
            jobId, workerId, status.getCode(), progress, "Worker started processing", eventTime);

    assertThatExceptionOfType(IllegalJobStatusUpdateException.class)
        .isThrownBy(() -> jobStatusUpdateService.apply(update))
        .withMessage("Worker is not allowed to publish status '" + status.getCode() + "'.");
  }

  private static Stream<String> jobInvalidWorkerIdValues() {
    return Stream.of(null, "", " ");
  }

  @ParameterizedTest
  @MethodSource("jobInvalidWorkerIdValues")
  void testApplyNullOrBlankWorkerId(String workerId) {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var response = createOrReuseDraftJob(userId, sessionId, jobType);
    var jobId = response.jobId();
    jobFixture.forceStatus(jobId, SUBMITTED);
    var eventTime = BASE_EVENT_TIME;
    var progress = 10;
    var update =
        new JobStatusUpdateDTO(
            jobId,
            workerId,
            IN_PROGRESS.getCode(),
            progress,
            "Worker started processing",
            eventTime);

    assertThatExceptionOfType(IllegalJobStatusUpdateException.class)
        .isThrownBy(() -> jobStatusUpdateService.apply(update))
        .withMessage("workerId is required for IN_PROGRESS status update.");
  }

  private record Transition(JobStatus from, JobStatus to, int progress) {}

  private static Stream<Transition> jobInvalidStateTransitions() {
    return Stream.of(
        new Transition(DRAFT, IN_PROGRESS, 10),
        new Transition(IN_PROGRESS, CANCELLED, 10),
        new Transition(IN_PROGRESS, VALIDATION_FAILED, 10),
        new Transition(SUBMITTED, DONE, 100),
        new Transition(VALIDATION_FAILED, ERROR, 10));
  }

  @ParameterizedTest
  @MethodSource("jobInvalidStateTransitions")
  void testApplyStateTransition(Transition transition) {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var response = createOrReuseDraftJob(userId, sessionId, jobType);
    var jobId = response.jobId();
    var from = transition.from;
    jobFixture.forceStatus(jobId, from);
    var eventTime = BASE_EVENT_TIME;
    var to = transition.to;
    var progress = transition.progress;
    var workerId = newWorkerId();
    var update =
        new JobStatusUpdateDTO(
            jobId, workerId, to.getCode(), progress, "Worker started processing", eventTime);

    assertThatExceptionOfType(IllegalJobStatusUpdateException.class)
        .isThrownBy(() -> jobStatusUpdateService.apply(update))
        .withMessage(
            "Illegal status transition from '" + from.getCode() + "' to '" + to.getCode() + "'.");
  }

  @Test
  void testApplyNullUpdate() {
    assertThatExceptionOfType(IllegalJobStatusUpdateException.class)
        .isThrownBy(() -> jobStatusUpdateService.apply(null))
        .withMessage("Status update must not be null.");
  }

  @Test
  void testApplyAnotherWorkerOwnership() {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var response = createOrReuseDraftJob(userId, sessionId, jobType);
    var jobId = response.jobId();
    jobFixture.forceStatus(jobId, SUBMITTED);
    var firstWorkerId = newWorkerId();
    var eventTime = BASE_EVENT_TIME;
    var progress = 1;
    var firstUpdate =
        new JobStatusUpdateDTO(
            jobId,
            firstWorkerId,
            IN_PROGRESS.getCode(),
            progress,
            "Worker started processing",
            eventTime);
    var result = jobStatusUpdateService.apply(firstUpdate);
    assertThat(result.jobId()).isEqualTo(jobId);
    assertThat(result.status()).isEqualTo(IN_PROGRESS);
    assertThat(result.userId()).isEqualTo(userId);

    var secondWorkerId = newWorkerId();
    var secondUpdate =
        new JobStatusUpdateDTO(
            jobId,
            secondWorkerId,
            IN_PROGRESS.getCode(),
            progress,
            "Worker started processing",
            eventTime);

    assertThatExceptionOfType(IllegalJobStatusUpdateException.class)
        .isThrownBy(() -> jobStatusUpdateService.apply(secondUpdate))
        .withMessage("Job '" + jobId + "' is already owned by another worker.");
  }

  @ParameterizedTest
  @MethodSource("jobInvalidWorkerIdValues")
  void testApplyInvalidWorkerOwnership(String workerId) {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var response = createOrReuseDraftJob(userId, sessionId, jobType);
    var jobId = response.jobId();
    jobFixture.forceStatus(jobId, CANCEL_REQUESTED);
    var firstWorkerId = newWorkerId();
    jobFixture.forceWorkerId(jobId, firstWorkerId);
    var eventTime = BASE_EVENT_TIME;
    var progress = 1;

    var update =
        new JobStatusUpdateDTO(
            jobId,
            workerId,
            CANCELLED.getCode(),
            progress,
            "Worker cancelled processing",
            eventTime);

    assertThatExceptionOfType(IllegalJobStatusUpdateException.class)
        .isThrownBy(() -> jobStatusUpdateService.apply(update))
        .withMessage(
            "workerId is required because job '" + jobId + "' is already owned by worker.");
  }

  private static String newSessionId() {
    return "session-" + UUID.randomUUID();
  }

  private static String newWorkerId() {
    return "worker-" + UUID.randomUUID();
  }

  private JobCreationResult createOrReuseDraftJob(long userId, String sessionId, String jobType) {
    return jobCreationService.createOrReuseDraftJob(userId, sessionId, jobType);
  }
}

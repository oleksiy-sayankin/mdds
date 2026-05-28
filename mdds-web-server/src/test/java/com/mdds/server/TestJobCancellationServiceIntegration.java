/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.mdds.domain.JobStatus;
import com.mdds.dto.CancelJobDTO;
import com.mdds.queue.QueueClient;
import com.mdds.server.jpa.JobsRepository;
import com.mdds.server.support.JobTestFixture;
import java.time.Duration;
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
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@SpringBootTest(properties = {"spring.config.import=classpath:test-job-profiles.yml"})
@Testcontainers
@Import(JobTestFixture.class)
class TestJobCancellationServiceIntegration {

  @Autowired private JobCreationService jobCreationService;
  @Autowired private UserLookupService userLookupService;
  @Autowired private JobCancellationService jobCancellationService;
  @Autowired private JobsRepository jobsRepository;

  @MockitoBean(name = "cancelQueueClient")
  private QueueClient cancelQueueClient;

  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17")
          .withDatabaseName("mdds")
          .withUsername("mdds")
          .withPassword("mdds123");

  @Container
  private static final RabbitMQContainer RABBIT_MQ =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672)
          .waitingFor(Wait.forListeningPort())
          .withStartupTimeout(Duration.ofSeconds(30));

  @DynamicPropertySource
  static void initProps(DynamicPropertyRegistry registry) {
    registry.add("mdds.rabbitmq.host", RABBIT_MQ::getHost);
    registry.add("mdds.rabbitmq.port", RABBIT_MQ::getAmqpPort);
    registry.add("mdds.rabbitmq.user", RABBIT_MQ::getAdminUsername);
    registry.add("mdds.rabbitmq.password", RABBIT_MQ::getAdminPassword);
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
  void testCancellation(String login) {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(login);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, sessionId, jobType).jobId();
    var workerId = newWorkerId();
    var job = jobsRepository.findById(jobId).orElseThrow();
    job.setWorkerId(workerId);
    job.setStatus(JobStatus.IN_PROGRESS);
    jobsRepository.save(job);

    jobCancellationService.cancel(userId, jobId);
    job = jobsRepository.findById(jobId).orElseThrow();
    var queueName = "cancel.queue-" + workerId;
    assertThat(job.getStatus()).isEqualTo(JobStatus.CANCEL_REQUESTED);
    verify(cancelQueueClient)
        .publish(
            eq(queueName),
            argThat(
                message ->
                    message.payload().equals(new CancelJobDTO(jobId))
                        && message.headers().isEmpty()));
  }

  private static Stream<JobStatus> jobTerminalStatusValues() {
    return Stream.of(
        JobStatus.CANCELLED, JobStatus.DONE, JobStatus.ERROR, JobStatus.VALIDATION_FAILED);
  }

  @ParameterizedTest
  @MethodSource("jobTerminalStatusValues")
  void testCancellationInTerminalJobState(JobStatus jobStatus) {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, sessionId, jobType).jobId();
    var workerId = newWorkerId();
    var job = jobsRepository.findById(jobId);

    job.ifPresentOrElse(
        value -> {
          value.setStatus(jobStatus);
          value.setWorkerId(workerId);
          jobsRepository.save(value);
        },
        () -> {
          throw new AssertionError("Value is missing");
        });

    assertThatExceptionOfType(JobIsInTerminalStateException.class)
        .isThrownBy(() -> jobCancellationService.cancel(userId, jobId))
        .withMessage(
            "Job '"
                + jobId
                + "' is in terminal state '"
                + jobStatus.getCode()
                + "' and cancellation is not allowed.");
    verify(cancelQueueClient, never()).publish(any(), any());
  }

  private static Stream<JobStatus> jobInvalidStatusValues() {
    return Stream.of(JobStatus.DRAFT, JobStatus.SUBMITTED);
  }

  @ParameterizedTest
  @MethodSource("jobInvalidStatusValues")
  void testCancellationInvalidJobState(JobStatus jobStatus) {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, sessionId, jobType).jobId();
    var workerId = newWorkerId();
    var job = jobsRepository.findById(jobId);

    job.ifPresentOrElse(
        value -> {
          value.setStatus(jobStatus);
          value.setWorkerId(workerId);
          jobsRepository.save(value);
        },
        () -> {
          throw new AssertionError("Value is missing");
        });

    assertThatExceptionOfType(JobIsNotRunningException.class)
        .isThrownBy(() -> jobCancellationService.cancel(userId, jobId))
        .withMessage(
            "Job '"
                + jobId
                + "' is in state '"
                + jobStatus.getCode()
                + "' and cancellation is supported only for 'IN_PROGRESS' jobs.");
    verify(cancelQueueClient, never()).publish(any(), any());
  }

  @Test
  void testCancellationJobFromOtherUser() {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var adminUserId = userLookupService.findUserId(ADMIN);
    var jobId = jobCreationService.createOrReuseDraftJob(adminUserId, sessionId, jobType).jobId();
    var workerId = newWorkerId();
    var job = jobsRepository.findById(jobId);

    job.ifPresentOrElse(
        value -> {
          value.setWorkerId(workerId);
          jobsRepository.save(value);
        },
        () -> {
          throw new AssertionError("Value is missing");
        });

    var guestUserId = userLookupService.findUserId(GUEST);
    assertThatExceptionOfType(JobDoesNotExistException.class)
        .isThrownBy(() -> jobCancellationService.cancel(guestUserId, jobId))
        .withMessage("Job with id '" + jobId + "' does not exist.");
    verify(cancelQueueClient, never()).publish(any(), any());
  }

  @Test
  void testCancellationNoWorker() {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, sessionId, jobType).jobId();
    var job = jobsRepository.findById(jobId);

    job.ifPresentOrElse(
        value -> {
          value.setStatus(JobStatus.IN_PROGRESS);
          jobsRepository.save(value);
        },
        () -> {
          throw new AssertionError("Value is missing");
        });

    assertThatExceptionOfType(JobHasNoWorkerAssignedException.class)
        .isThrownBy(() -> jobCancellationService.cancel(userId, jobId))
        .withMessage("Job '" + jobId + "' is in state 'IN_PROGRESS' but workerId is not assigned.");
    verify(cancelQueueClient, never()).publish(any(), any());
  }

  @Test
  void testCancellationNoJob() {
    var jobId = "invalid-job-id";
    var userId = userLookupService.findUserId(GUEST);
    assertThatExceptionOfType(JobDoesNotExistException.class)
        .isThrownBy(() -> jobCancellationService.cancel(userId, jobId))
        .withMessage("Job with id '" + jobId + "' does not exist.");
    verify(cancelQueueClient, never()).publish(any(), any());
  }

  @Test
  void testCancellationWhenCancelAlreadyRequested() {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, sessionId, jobType).jobId();
    var workerId = newWorkerId();
    var job = jobsRepository.findById(jobId).orElseThrow();
    job.setStatus(JobStatus.CANCEL_REQUESTED);
    job.setWorkerId(workerId);
    jobsRepository.save(job);

    jobCancellationService.cancel(userId, jobId);
    verify(cancelQueueClient, never()).publish(any(), any());
  }

  private static String newSessionId() {
    return "session-" + UUID.randomUUID();
  }

  private static String newWorkerId() {
    return "worker-" + UUID.randomUUID();
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.mdds.dto.CreateJobRequestDTO;
import com.mdds.dto.JobIdResponseDTO;
import com.mdds.dto.JobStatus;
import com.mdds.dto.UnknownJobTypeException;
import com.mdds.server.jpa.JobsRepository;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
class TestCreateOrReuseDraftJobIntegration {

  @Autowired private JobsRepository jobsRepository;
  @Autowired private UserLookupService userLookupService;
  @Autowired private JobController controller;

  private static final Pattern UUID_REGEX_PATTERN =
      Pattern.compile("^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$");

  private static final String GUEST = "guest";
  private static final String ADMIN = "admin";

  @Container
  private static final PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:17")
          .withDatabaseName("mdds")
          .withUsername("mdds")
          .withPassword("mdds123");

  @DynamicPropertySource
  static void registerProps(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", postgres::getJdbcUrl);
    registry.add("spring.datasource.username", postgres::getUsername);
    registry.add("spring.datasource.password", postgres::getPassword);
  }

  @Test
  void testCreateOrReuseDraftJobCreatesNewJobForNewSession() {
    var session = newSessionId();
    var jobId = createOrReuseDraftJob(GUEST, session);
    assertValidJobId(jobId);
    assertSingleJobRow(GUEST, session);
  }

  @Test
  void testCreateOrReuseDraftJobThrowsExceptionForInvalidUser() {
    var session = newSessionId();
    assertThatExceptionOfType(UnknownUserException.class)
        .isThrownBy(() -> createOrReuseDraftJob("invalid_user", session))
        .withMessage("Unknown user login: invalid_user.");
  }

  @Test
  void testCreateOrReuseDraftJobThrowsExceptionForInvalidJobType() {
    var session = newSessionId();
    var createJobRequest = new CreateJobRequestDTO("wrong_job_type");
    assertThatExceptionOfType(UnknownJobTypeException.class)
        .isThrownBy(() -> createOrReuseDraftJob(GUEST, session, createJobRequest))
        .withMessage("Unknown or unsupported job type: wrong_job_type.");
  }

  @Test
  void testCreateOrReuseDraftJobThrowsExceptionForEmptySessionId() {
    var createJobRequest = new CreateJobRequestDTO("solving_slae");
    assertThatExceptionOfType(UploadSessionIdIsNullOrBlankException.class)
        .isThrownBy(() -> createOrReuseDraftJob(GUEST, "", createJobRequest))
        .withMessage("Upload session id is null or blank.");
  }

  @Test
  void testCreateOrReuseDraftJobThrowsExceptionForNullSessionId() {
    var createJobRequest = new CreateJobRequestDTO("solving_slae");
    assertThatExceptionOfType(UploadSessionIdIsNullOrBlankException.class)
        .isThrownBy(() -> createOrReuseDraftJob(GUEST, null, createJobRequest))
        .withMessage("Upload session id is null or blank.");
  }

  @Test
  void testCreateOrReuseDraftJobThrowsExceptionForJobTypeConflict() {
    var session = newSessionId();
    var jobTypeA = "solving_slae";
    var jobTypeB = "solving_slae_parallel";
    createOrReuseDraftJob(GUEST, session, new CreateJobRequestDTO(jobTypeA));
    var createJobRequestB = new CreateJobRequestDTO(jobTypeB);
    assertThatExceptionOfType(JobTypeConflictException.class)
        .isThrownBy(() -> createOrReuseDraftJob(GUEST, session, createJobRequestB))
        .withMessage(
            "A draft job already exists for upload session id '"
                + session
                + "' with job type '"
                + jobTypeA
                + "', which does not match requested job type '"
                + jobTypeB
                + "'.");
  }

  @Test
  void testCreateOrReuseDraftJobThrowsExceptionForJobNotInDraft() {
    var session = newSessionId();
    var jobType = "solving_slae";
    var createJobRequest = new CreateJobRequestDTO(jobType);
    var response = createOrReuseDraftJob(GUEST, session, createJobRequest);
    var jobId = response.getJobId();
    var status = JobStatus.SUBMITTED;
    var jobResponse = jobsRepository.findById(jobId);
    jobResponse.ifPresent(
        job -> {
          job.setStatus(status);
          jobsRepository.save(job);
        });
    assertThatExceptionOfType(JobIsNotDraftException.class)
        .isThrownBy(() -> createOrReuseDraftJob(GUEST, session, createJobRequest))
        .withMessage(
            "Upload session id '"
                + session
                + "' is already bound to job '"
                + jobId
                + "' with status '"
                + status
                + "'. A new upload session id is required.");
  }

  @Test
  void testCreateOrReuseDraftJobCreatesIndependentJobsForDifferentUsersOnSameSession() {
    var session = newSessionId();

    var guestJobId = createOrReuseDraftJob(GUEST, session);
    assertValidJobId(guestJobId);
    assertSingleJobRow(GUEST, session);

    var adminJobId = createOrReuseDraftJob(ADMIN, session);
    assertValidJobId(adminJobId);
    assertSingleJobRow(ADMIN, session);

    assertThat(guestJobId.getJobId()).isNotEqualTo(adminJobId.getJobId());
  }

  @Test
  void testCreateOrReuseDraftJobReturnsSameJobIdForSameUserAndSession() {
    var session = newSessionId();
    var firstJobId = createOrReuseDraftJob(GUEST, session);
    assertValidJobId(firstJobId);
    var secondJobId = createOrReuseDraftJob(GUEST, session);
    assertValidJobId(secondJobId);
    assertThat(firstJobId.getJobId()).isEqualTo(secondJobId.getJobId());
    assertSingleJobRow(GUEST, session);
  }

  @Test
  void testCreateOrReuseDraftJobReturnsDifferentJobIdsForDifferentSessionsOfSameUser() {
    var firstSession = newSessionId();
    var firstJobId = createOrReuseDraftJob(GUEST, firstSession);
    assertValidJobId(firstJobId);
    var secondSession = newSessionId();
    var secondJobId = createOrReuseDraftJob(GUEST, secondSession);
    assertValidJobId(secondJobId);
    assertThat(firstJobId.getJobId()).isNotEqualTo(secondJobId.getJobId());

    assertSingleJobRow(GUEST, firstSession);
    assertSingleJobRow(GUEST, secondSession);
  }

  @Test
  void testCreateOrReuseDraftJobCreatesDistinctJobsForManyDifferentSessions() {
    final var numRequests = 100;
    var jobIds = new HashSet<String>();
    var prefix = newSessionId();
    IntStream.range(0, numRequests)
        .forEach(i -> jobIds.add(createOrReuseDraftJob(GUEST, prefix + "-" + i).getJobId()));
    assertThat(jobIds).hasSize(numRequests);
    IntStream.range(0, numRequests).forEach(i -> assertSingleJobRow(GUEST, prefix + "-" + i));
  }

  @Test
  void testCreateOrReuseDraftJobIsRaceSafeWhenCalledConcurrentlyForNewSession()
      throws ExecutionException, InterruptedException, TimeoutException {
    var barrier = new CyclicBarrier(2);
    var session = newSessionId();

    Callable<JobIdResponseDTO> callable =
        () -> {
          barrier.await(5, TimeUnit.SECONDS);
          return createOrReuseDraftJob(GUEST, session);
        };

    try (var executorService = Executors.newFixedThreadPool(2)) {
      var firstFuture = executorService.submit(callable);
      var secondFuture = executorService.submit(callable);

      var firstJobId = firstFuture.get(10, TimeUnit.SECONDS);
      var secondJobId = secondFuture.get(10, TimeUnit.SECONDS);

      assertValidJobId(firstJobId);
      assertValidJobId(secondJobId);

      assertThat(firstJobId.getJobId()).isEqualTo(secondJobId.getJobId());
      assertSingleJobRow(GUEST, session);
    }
  }

  @Test
  void testCreateOrReuseDraftJobReturnsExistingJobIdWhenCalledConcurrentlyForExistingSession()
      throws ExecutionException, InterruptedException, TimeoutException {
    var session = newSessionId();
    final var initialJobId = createOrReuseDraftJob(GUEST, session);
    assertValidJobId(initialJobId);

    var barrier = new CyclicBarrier(2);

    Callable<JobIdResponseDTO> callable =
        () -> {
          barrier.await(5, TimeUnit.SECONDS);
          return createOrReuseDraftJob(GUEST, session);
        };

    try (var executorService = Executors.newFixedThreadPool(2)) {
      var firstFuture = executorService.submit(callable);
      var secondFuture = executorService.submit(callable);

      var firstJobId = firstFuture.get(10, TimeUnit.SECONDS);
      var secondJobId = secondFuture.get(10, TimeUnit.SECONDS);

      assertThat(initialJobId.getJobId()).isEqualTo(firstJobId.getJobId());
      assertThat(initialJobId.getJobId()).isEqualTo(secondJobId.getJobId());
      assertSingleJobRow(GUEST, session);
    }
  }

  @Test
  void testCreateOrReuseDraftJobRemainsIdempotentUnderConcurrentRepeatedRequests()
      throws InterruptedException, ExecutionException, TimeoutException {
    var session = newSessionId();
    final var initialJobId = createOrReuseDraftJob(GUEST, session);
    assertValidJobId(initialJobId);

    var barrier = new CyclicBarrier(2);

    Callable<Set<String>> callable =
        () -> {
          barrier.await(5, TimeUnit.SECONDS);
          var jobIds = new HashSet<String>();
          IntStream.range(0, 100)
              .forEach(i -> jobIds.add(createOrReuseDraftJob(GUEST, session).getJobId()));
          return jobIds;
        };

    try (var executorService = Executors.newFixedThreadPool(2)) {
      var firstFuture = executorService.submit(callable);
      var secondFuture = executorService.submit(callable);
      var firstJobIds = firstFuture.get(10, TimeUnit.SECONDS);
      var secondJobIds = secondFuture.get(10, TimeUnit.SECONDS);
      assertThat(firstJobIds).hasSize(1);
      assertThat(secondJobIds).hasSize(1);
      assertThat(firstJobIds).containsOnly(initialJobId.getJobId());
      assertThat(secondJobIds).containsOnly(initialJobId.getJobId());
      assertSingleJobRow(GUEST, session);
    }
  }

  private static String newSessionId() {
    return "session-" + UUID.randomUUID();
  }

  private static boolean isValidUUID(String uuidString) {
    if (uuidString == null) {
      return false;
    }
    return UUID_REGEX_PATTERN.matcher(uuidString).matches();
  }

  private JobIdResponseDTO createOrReuseDraftJob(String user, String session) {
    return controller
        .createOrReuseDraftJob(user, session, new CreateJobRequestDTO("solving_slae"))
        .getBody();
  }

  private JobIdResponseDTO createOrReuseDraftJob(
      String user, String session, CreateJobRequestDTO createJobRequestDTO) {
    return controller.createOrReuseDraftJob(user, session, createJobRequestDTO).getBody();
  }

  private void assertValidJobId(JobIdResponseDTO response) {
    assertThat(response).isNotNull();
    assertThat(response.getJobId()).isNotBlank();
    assertThat(isValidUUID(response.getJobId())).isTrue();
  }

  private void assertSingleJobRow(String user, String session) {
    long userId = userLookupService.findUserId(user);
    assertThat(jobsRepository.countByUserIdAndUploadSessionId(userId, session)).isEqualTo(1);
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.mdds.domain.JobStatus;
import com.mdds.server.support.JobTestFixture;
import java.time.Duration;
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
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@SpringBootTest(properties = {"spring.config.import=classpath:test-job-profiles.yml"})
@Testcontainers
@Import(JobTestFixture.class)
class TestJobCreationServiceIntegration {
  @Autowired private JobCreationService jobCreationService;
  @Autowired private UserLookupService userLookupService;
  @Autowired private JobTestFixture jobFixture;

  private static final Pattern UUID_REGEX_PATTERN =
      Pattern.compile("^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$");

  private static final String GUEST = "guest";
  private static final String ADMIN = "admin";

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
  static void registerProps(DynamicPropertyRegistry registry) {
    registry.add("mdds.rabbitmq.host", RABBIT_MQ::getHost);
    registry.add("mdds.rabbitmq.port", RABBIT_MQ::getAmqpPort);
    registry.add("mdds.rabbitmq.user", RABBIT_MQ::getAdminUsername);
    registry.add("mdds.rabbitmq.password", RABBIT_MQ::getAdminPassword);
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
  }

  @Test
  void testCreateOrReuseDraftJobCreatesNewJobForNewSession() {
    var sessionId = newSessionId();
    var userId = userLookupService.findUserId(GUEST);
    var result = createOrReuseDraftJob(userId, sessionId);
    assertValidJobId(result.jobId());
    assertSingleJobRow(userId, sessionId);
  }

  @Test
  void testCreateOrReuseDraftJobThrowsExceptionForInvalidJobType() {
    var sessionId = newSessionId();
    var userId = userLookupService.findUserId(GUEST);
    assertThatExceptionOfType(UnknownOrUnsupportedJobTypeException.class)
        .isThrownBy(() -> createOrReuseDraftJob(userId, sessionId, "wrong_job_type"))
        .withMessage("Unknown or unsupported job type: wrong_job_type.");
  }

  @Test
  void testCreateOrReuseDraftJobThrowsExceptionForEmptySessionId() {
    var userId = userLookupService.findUserId(GUEST);
    assertThatExceptionOfType(UploadSessionIdIsNullOrBlankException.class)
        .isThrownBy(() -> createOrReuseDraftJob(userId, "", "solving_slae"))
        .withMessage("Upload session id is null or blank.");
  }

  @Test
  void testCreateOrReuseDraftJobThrowsExceptionForNullSessionId() {
    var userId = userLookupService.findUserId(GUEST);
    assertThatExceptionOfType(UploadSessionIdIsNullOrBlankException.class)
        .isThrownBy(() -> createOrReuseDraftJob(userId, null, "solving_slae"))
        .withMessage("Upload session id is null or blank.");
  }

  @Test
  void testCreateOrReuseDraftJobThrowsExceptionForJobTypeConflict() {
    var sessionId = newSessionId();
    var userId = userLookupService.findUserId(GUEST);
    var jobTypeA = "solving_slae";
    var jobTypeB = "solving_slae_parallel";
    createOrReuseDraftJob(userId, sessionId, jobTypeA);
    assertThatExceptionOfType(JobTypeConflictException.class)
        .isThrownBy(() -> createOrReuseDraftJob(userId, sessionId, jobTypeB))
        .withMessage(
            "A draft job already exists for upload session id '"
                + sessionId
                + "' with job type '"
                + jobTypeA
                + "', which does not match requested job type '"
                + jobTypeB
                + "'.");
  }

  @Test
  void testCreateOrReuseDraftJobThrowsExceptionForJobNotInDraft() {
    var sessionId = newSessionId();
    var userId = userLookupService.findUserId(GUEST);
    var jobType = "solving_slae";
    var response = createOrReuseDraftJob(userId, sessionId, jobType);
    var jobId = response.jobId();
    var status = JobStatus.SUBMITTED;
    jobFixture.forceStatus(jobId, status);
    assertThatExceptionOfType(JobIsNotDraftException.class)
        .isThrownBy(() -> createOrReuseDraftJob(userId, sessionId, jobType))
        .withMessage(
            "Upload session id '"
                + sessionId
                + "' is already bound to job '"
                + jobId
                + "' with status '"
                + status
                + "'. A new upload session id is required.");
  }

  @Test
  void testCreateOrReuseDraftJobCreatesIndependentJobsForDifferentUsersOnSameSession() {
    var sessionId = newSessionId();
    var guestUserId = userLookupService.findUserId(GUEST);

    var guestJobId = createOrReuseDraftJob(guestUserId, sessionId).jobId();
    assertValidJobId(guestJobId);
    assertSingleJobRow(guestUserId, sessionId);

    var adminUserId = userLookupService.findUserId(ADMIN);
    var adminJobId = createOrReuseDraftJob(adminUserId, sessionId).jobId();
    assertValidJobId(adminJobId);
    assertSingleJobRow(adminUserId, sessionId);

    assertThat(guestJobId).isNotEqualTo(adminJobId);
  }

  @Test
  void testCreateOrReuseDraftJobReturnsSameJobIdForSameUserAndSession() {
    var sessionId = newSessionId();
    var userId = userLookupService.findUserId(GUEST);
    var firstJobId = createOrReuseDraftJob(userId, sessionId).jobId();
    assertValidJobId(firstJobId);
    var secondJobId = createOrReuseDraftJob(userId, sessionId).jobId();
    assertValidJobId(secondJobId);
    assertThat(firstJobId).isEqualTo(secondJobId);
    assertSingleJobRow(userId, sessionId);
  }

  @Test
  void testCreateOrReuseDraftJobReturnsDifferentJobIdsForDifferentSessionsOfSameUser() {
    var firstSession = newSessionId();
    var userId = userLookupService.findUserId(GUEST);
    var firstJobId = createOrReuseDraftJob(userId, firstSession).jobId();
    assertValidJobId(firstJobId);
    var secondSession = newSessionId();
    var secondJobId = createOrReuseDraftJob(userId, secondSession).jobId();
    assertValidJobId(secondJobId);
    assertThat(firstJobId).isNotEqualTo(secondJobId);

    assertSingleJobRow(userId, firstSession);
    assertSingleJobRow(userId, secondSession);
  }

  @Test
  void testCreateOrReuseDraftJobCreatesDistinctJobsForManyDifferentSessions() {
    final var numRequests = 100;
    var jobIds = new HashSet<String>();
    var prefix = newSessionId();
    var userId = userLookupService.findUserId(GUEST);
    IntStream.range(0, numRequests)
        .forEach(i -> jobIds.add(createOrReuseDraftJob(userId, prefix + "-" + i).jobId()));
    assertThat(jobIds).hasSize(numRequests);
    IntStream.range(0, numRequests).forEach(i -> assertSingleJobRow(userId, prefix + "-" + i));
  }

  @Test
  void testCreateOrReuseDraftJobIsRaceSafeWhenCalledConcurrentlyForNewSession()
      throws ExecutionException, InterruptedException, TimeoutException {
    var barrier = new CyclicBarrier(2);
    var sessionId = newSessionId();
    var userId = userLookupService.findUserId(GUEST);

    Callable<String> callable =
        () -> {
          barrier.await(5, TimeUnit.SECONDS);
          return createOrReuseDraftJob(userId, sessionId).jobId();
        };

    try (var executorService = Executors.newFixedThreadPool(2)) {
      var firstFuture = executorService.submit(callable);
      var secondFuture = executorService.submit(callable);

      var firstJobId = firstFuture.get(10, TimeUnit.SECONDS);
      var secondJobId = secondFuture.get(10, TimeUnit.SECONDS);

      assertValidJobId(firstJobId);
      assertValidJobId(secondJobId);

      assertThat(firstJobId).isEqualTo(secondJobId);
      assertSingleJobRow(userId, sessionId);
    }
  }

  @Test
  void testCreateOrReuseDraftJobReturnsExistingJobIdWhenCalledConcurrentlyForExistingSession()
      throws ExecutionException, InterruptedException, TimeoutException {
    var sessionId = newSessionId();
    var userId = userLookupService.findUserId(GUEST);
    final var initialJobId = createOrReuseDraftJob(userId, sessionId).jobId();
    assertValidJobId(initialJobId);

    var barrier = new CyclicBarrier(2);

    Callable<String> callable =
        () -> {
          barrier.await(5, TimeUnit.SECONDS);
          return createOrReuseDraftJob(userId, sessionId).jobId();
        };

    try (var executorService = Executors.newFixedThreadPool(2)) {
      var firstFuture = executorService.submit(callable);
      var secondFuture = executorService.submit(callable);

      var firstJobId = firstFuture.get(10, TimeUnit.SECONDS);
      var secondJobId = secondFuture.get(10, TimeUnit.SECONDS);

      assertThat(initialJobId).isEqualTo(firstJobId);
      assertThat(initialJobId).isEqualTo(secondJobId);
      assertSingleJobRow(userId, sessionId);
    }
  }

  @Test
  void testCreateOrReuseDraftJobRemainsIdempotentUnderConcurrentRepeatedRequests()
      throws InterruptedException, ExecutionException, TimeoutException {
    var sessionId = newSessionId();
    var userId = userLookupService.findUserId(GUEST);
    final var initialJobId = createOrReuseDraftJob(userId, sessionId).jobId();
    assertValidJobId(initialJobId);

    var barrier = new CyclicBarrier(2);

    Callable<Set<String>> callable =
        () -> {
          barrier.await(5, TimeUnit.SECONDS);
          var jobIds = new HashSet<String>();
          IntStream.range(0, 100)
              .forEach(i -> jobIds.add(createOrReuseDraftJob(userId, sessionId).jobId()));
          return jobIds;
        };

    try (var executorService = Executors.newFixedThreadPool(2)) {
      var firstFuture = executorService.submit(callable);
      var secondFuture = executorService.submit(callable);
      var firstJobIds = firstFuture.get(10, TimeUnit.SECONDS);
      var secondJobIds = secondFuture.get(10, TimeUnit.SECONDS);
      assertThat(firstJobIds).hasSize(1);
      assertThat(secondJobIds).hasSize(1);
      assertThat(firstJobIds).containsOnly(initialJobId);
      assertThat(secondJobIds).containsOnly(initialJobId);
      assertSingleJobRow(userId, sessionId);
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

  private JobCreationResult createOrReuseDraftJob(long userId, String sessionId) {
    return jobCreationService.createOrReuseDraftJob(userId, sessionId, "solving_slae");
  }

  private JobCreationResult createOrReuseDraftJob(long userId, String sessionId, String jobType) {
    return jobCreationService.createOrReuseDraftJob(userId, sessionId, jobType);
  }

  private void assertValidJobId(String jobId) {
    assertThat(jobId).isNotNull();
    assertThat(jobId).isNotBlank();
    assertThat(isValidUUID(jobId)).isTrue();
  }

  private void assertSingleJobRow(long userId, String sessionId) {
    assertThat(jobFixture.countByUserIdAndUploadSessionId(userId, sessionId)).isEqualTo(1);
  }
}

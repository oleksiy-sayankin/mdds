/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.mdds.dto.JobIdResponseDTO;
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
class TestServerInitJobControllerIntegration {

  @Autowired private JobsRepository jobsRepository;
  @Autowired private UserLookupService userLookupService;
  @Autowired private ServerInitJobController controller;

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
  void testInitJobCreatesNewJobForNewSession() {
    var session = newSessionId();
    var jobId = initJob(GUEST, session);
    assertValidJobId(jobId);
    assertSingleJobRow(GUEST, session);
  }

  @Test
  void testInitJobThrowsExceptionForInvalidUser() {
    var session = newSessionId();
    assertThatExceptionOfType(UnknownUserException.class)
        .isThrownBy(() -> initJob("invalid_user", session))
        .withMessage("Unknown user login: invalid_user");
  }

  @Test
  void testInitJobCreatesIndependentJobsForDifferentUsersOnSameSession() {
    var session = newSessionId();

    var guestJobId = initJob(GUEST, session);
    assertValidJobId(guestJobId);
    assertSingleJobRow(GUEST, session);

    var adminJobId = initJob(ADMIN, session);
    assertValidJobId(adminJobId);
    assertSingleJobRow(ADMIN, session);

    assertThat(guestJobId.getId()).isNotEqualTo(adminJobId.getId());
  }

  @Test
  void testInitJobReturnsSameJobIdForSameUserAndSession() {
    var session = newSessionId();
    var firstJobId = initJob(GUEST, session);
    assertValidJobId(firstJobId);
    var secondJobId = initJob(GUEST, session);
    assertValidJobId(secondJobId);
    assertThat(firstJobId.getId()).isEqualTo(secondJobId.getId());
    assertSingleJobRow(GUEST, session);
  }

  @Test
  void testInitJobReturnsDifferentJobIdsForDifferentSessionsOfSameUser() {
    var firstSession = newSessionId();
    var firstJobId = initJob(GUEST, firstSession);
    assertValidJobId(firstJobId);
    var secondSession = newSessionId();
    var secondJobId = initJob(GUEST, secondSession);
    assertValidJobId(secondJobId);
    assertThat(firstJobId.getId()).isNotEqualTo(secondJobId.getId());

    assertSingleJobRow(GUEST, firstSession);
    assertSingleJobRow(GUEST, secondSession);
  }

  @Test
  void testInitJobCreatesDistinctJobsForManyDifferentSessions() {
    final var numRequests = 100;
    var jobIds = new HashSet<String>();
    var prefix = newSessionId();
    IntStream.range(0, numRequests)
        .forEach(i -> jobIds.add(initJob(GUEST, prefix + "-" + i).getId()));
    assertThat(jobIds).hasSize(numRequests);
    IntStream.range(0, numRequests).forEach(i -> assertSingleJobRow(GUEST, prefix + "-" + i));
  }

  @Test
  void testInitJobIsRaceSafeWhenCalledConcurrentlyForNewSession()
      throws ExecutionException, InterruptedException, TimeoutException {
    var barrier = new CyclicBarrier(2);
    var session = newSessionId();

    Callable<JobIdResponseDTO> callable =
        () -> {
          barrier.await(5, TimeUnit.SECONDS);
          return initJob(GUEST, session);
        };

    try (var executorService = Executors.newFixedThreadPool(2)) {
      var firstFuture = executorService.submit(callable);
      var secondFuture = executorService.submit(callable);

      var firstJobId = firstFuture.get(10, TimeUnit.SECONDS);
      var secondJobId = secondFuture.get(10, TimeUnit.SECONDS);

      assertValidJobId(firstJobId);
      assertValidJobId(secondJobId);

      assertThat(firstJobId.getId()).isEqualTo(secondJobId.getId());
      assertSingleJobRow(GUEST, session);
    }
  }

  @Test
  void testInitJobReturnsExistingJobIdWhenCalledConcurrentlyForExistingSession()
      throws ExecutionException, InterruptedException, TimeoutException {
    var session = newSessionId();
    final var initialJobId = initJob(GUEST, session);
    assertValidJobId(initialJobId);

    var barrier = new CyclicBarrier(2);

    Callable<JobIdResponseDTO> callable =
        () -> {
          barrier.await(5, TimeUnit.SECONDS);
          return initJob(GUEST, session);
        };

    try (var executorService = Executors.newFixedThreadPool(2)) {
      var firstFuture = executorService.submit(callable);
      var secondFuture = executorService.submit(callable);

      var firstJobId = firstFuture.get(10, TimeUnit.SECONDS);
      var secondJobId = secondFuture.get(10, TimeUnit.SECONDS);

      assertThat(initialJobId.getId()).isEqualTo(firstJobId.getId());
      assertThat(initialJobId.getId()).isEqualTo(secondJobId.getId());
      assertSingleJobRow(GUEST, session);
    }
  }

  @Test
  void testInitJobRemainsIdempotentUnderConcurrentRepeatedRequests()
      throws InterruptedException, ExecutionException, TimeoutException {
    var session = newSessionId();
    final var initialJobId = initJob(GUEST, session);
    assertValidJobId(initialJobId);

    var barrier = new CyclicBarrier(2);

    Callable<Set<String>> callable =
        () -> {
          barrier.await(5, TimeUnit.SECONDS);
          var jobIds = new HashSet<String>();
          IntStream.range(0, 100).forEach(i -> jobIds.add(initJob(GUEST, session).getId()));
          return jobIds;
        };

    try (var executorService = Executors.newFixedThreadPool(2)) {
      var firstFuture = executorService.submit(callable);
      var secondFuture = executorService.submit(callable);
      var firstJobIds = firstFuture.get(10, TimeUnit.SECONDS);
      var secondJobIds = secondFuture.get(10, TimeUnit.SECONDS);
      assertThat(firstJobIds).hasSize(1);
      assertThat(secondJobIds).hasSize(1);
      assertThat(firstJobIds).containsOnly(initialJobId.getId());
      assertThat(secondJobIds).containsOnly(initialJobId.getId());
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

  private JobIdResponseDTO initJob(String user, String session) {
    return controller.initJob(user, session);
  }

  private void assertValidJobId(JobIdResponseDTO response) {
    assertThat(response).isNotNull();
    assertThat(response.getId()).isNotBlank();
    assertThat(isValidUUID(response.getId())).isTrue();
  }

  private void assertSingleJobRow(String user, String session) {
    long userId = userLookupService.findUserIdOrGuest(user);
    assertThat(jobsRepository.countByUserIdAndUploadSessionId(userId, session)).isEqualTo(1);
  }
}

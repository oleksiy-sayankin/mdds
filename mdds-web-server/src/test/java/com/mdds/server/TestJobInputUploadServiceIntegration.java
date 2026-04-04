/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.mdds.domain.JobProfiles;
import com.mdds.domain.JobStatus;
import com.mdds.domain.JobType;
import com.mdds.dto.CreateJobRequestDTO;
import com.mdds.dto.JobIdResponseDTO;
import com.mdds.server.jpa.JobsRepository;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;

import com.mdds.server.support.JobTestFixture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
@Import(JobTestFixture.class)
class TestJobInputUploadServiceIntegration {

  @Autowired private JobInputUploadService jobInputUploadService;
  @Autowired private ObjectStorageProperties objectStorageProperties;
  @Autowired private JobCreationService jobCreationService;
  @Autowired private UserLookupService userLookupService;
  @Autowired private JobTestFixture jobFixture;

  @Container
  private static final MinIOContainer MINIO_CONTAINER =
      new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z")
          .withUserName("testuser")
          .withPassword("testpassword");

  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17")
          .withDatabaseName("mdds")
          .withUsername("mdds")
          .withPassword("mdds123");

  @DynamicPropertySource
  static void initProps(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("mdds.object-storage.access-key", MINIO_CONTAINER::getUserName);
    registry.add("mdds.object-storage.secret-key", MINIO_CONTAINER::getPassword);
    registry.add("mdds.object-storage.public-endpoint", MINIO_CONTAINER::getS3URL);
  }

  private static final String GUEST = "guest";
  private static final String ADMIN = "admin";

  private static Stream<String> userLoginValues() {
    return Stream.of(GUEST, ADMIN);
  }

  @ParameterizedTest
  @MethodSource("userLoginValues")
  void testIssueUploadUrl(String login) {
    var session = newSessionId();
    var jobType = JobType.SOLVING_SLAE;
    var userId = userLookupService.findUserId(login);
    var response = createOrReuseDraftJob(userId, session, jobType);
    var jobId = response.jobId();

    var ttl = objectStorageProperties.presignPutTtl();

    var inputSlot = "matrix";
    var before = Instant.now();
    var result = jobInputUploadService.issueUploadUrl(userId, jobId, inputSlot);
    var after = Instant.now();

    assertThat(result.expiresAt()).isAfterOrEqualTo(before.plus(ttl).minusSeconds(1));
    assertThat(result.expiresAt()).isBeforeOrEqualTo(after.plus(ttl).plusSeconds(1));

    var url = result.uploadUrl();
    assertThat(url).isNotNull();
    var fileName =
        JobProfiles.forType(JobType.SOLVING_SLAE).inputArtifacts().get(inputSlot).fileName();
    assertThat(url.getPath()).contains(fileName);
  }

  private static Stream<Arguments> inputSlots() {
    return Stream.of(
        Arguments.of("matrix", "matrix"),
        Arguments.of("matrix ", "matrix"),
        Arguments.of(" matrix ", "matrix"),
        Arguments.of("MATRIX", "matrix"),
        Arguments.of("Matrix", "matrix"),
        Arguments.of("rhs", "rhs"));
  }

  @ParameterizedTest
  @MethodSource("inputSlots")
  void testIssueUploadUrlSlotNormalization(String inputSlot, String expectedSlot) {
    var session = newSessionId();
    var jobType = JobType.SOLVING_SLAE;
    var userId = userLookupService.findUserId(GUEST);
    var response = createOrReuseDraftJob(userId, session, jobType);
    var jobId = response.jobId();

    var ttl = objectStorageProperties.presignPutTtl();

    var before = Instant.now();
    var result = jobInputUploadService.issueUploadUrl(userId, jobId, inputSlot);
    var after = Instant.now();

    assertThat(result.expiresAt()).isAfterOrEqualTo(before.plus(ttl).minusSeconds(1));
    assertThat(result.expiresAt()).isBeforeOrEqualTo(after.plus(ttl).plusSeconds(1));

    var url = result.uploadUrl();
    assertThat(url).isNotNull();
    var fileName =
        JobProfiles.forType(JobType.SOLVING_SLAE).inputArtifacts().get(expectedSlot).fileName();
    assertThat(url.getPath()).contains(fileName);
  }

  @Test
  void testIssueUploadUrlInvalidJobId() {
    var jobId = "wrong_job_id";
    var userId = userLookupService.findUserId(GUEST);
    var inputSlot = "matrix";
    assertThatExceptionOfType(JobDoesNotExistException.class)
        .isThrownBy(() -> jobInputUploadService.issueUploadUrl(userId, jobId, inputSlot))
        .withMessage("Job with id '" + jobId + "' does not exist.");
  }

  @Test
  void testIssueUploadUrlJobBelongsToOtherUser() {
    var session = newSessionId();
    var jobType = JobType.SOLVING_SLAE;
    var adminId = userLookupService.findUserId(ADMIN);
    var response = createOrReuseDraftJob(adminId, session, jobType);
    var jobId = response.jobId();
    final var guestId = userLookupService.findUserId(GUEST);
    var inputSlot = "matrix";

    assertThatExceptionOfType(JobDoesNotExistException.class)
        .isThrownBy(() -> jobInputUploadService.issueUploadUrl(guestId, jobId, inputSlot))
        .withMessage("Job with id '" + jobId + "' does not exist.");
  }

  @Test
  void testIssueUploadUrlJobTypeNotSupported() {
    var session = newSessionId();
    var jobType = JobType.SOLVING_SLAE_PARALLEL;
    var userId = userLookupService.findUserId(GUEST);
    var response = createOrReuseDraftJob(userId, session, jobType);
    var jobId = response.jobId();
    var inputSlot = "matrix";

    assertThatExceptionOfType(InputUploadUrlNotSupportedForJobTypeException.class)
        .isThrownBy(() -> jobInputUploadService.issueUploadUrl(userId, jobId, inputSlot))
        .withMessage(
            "Input upload URL requests are not supported for the given jobType: '"
                + jobType.value()
                + "'.");
  }

  @Test
  void testIssueUploadUrlBlankInputSlot() {
    var session = newSessionId();
    var jobType = JobType.SOLVING_SLAE;
    var userId = userLookupService.findUserId(GUEST);
    var response = createOrReuseDraftJob(userId, session, jobType);
    var jobId = response.jobId();
    var inputSlot = "";

    assertThatExceptionOfType(InputSlotIsNullOrBlankException.class)
        .isThrownBy(() -> jobInputUploadService.issueUploadUrl(userId, jobId, inputSlot))
        .withMessage("Input slot is null or blank.");
  }

  @Test
  void testIssueUploadUrlInvalidInputJobState() {
    var session = newSessionId();
    var jobType = JobType.SOLVING_SLAE;
    var userId = userLookupService.findUserId(GUEST);
    var response = createOrReuseDraftJob(userId, session, jobType);
    var jobId = response.jobId();
    jobFixture.forceStatus(jobId, JobStatus.SUBMITTED);

    var inputSlot = "matrix";

    assertThatExceptionOfType(JobIsNotDraftException.class)
        .isThrownBy(() -> jobInputUploadService.issueUploadUrl(userId, jobId, inputSlot))
        .withMessage(
            "Job '"
                + jobId
                + "' is not in DRAFT state and no more input artifacts can be uploaded.");
  }

  @Test
  void testIssueUploadUrlUnknownInputSlot() {
    var session = newSessionId();
    var jobType = JobType.SOLVING_SLAE;
    var userId = userLookupService.findUserId(GUEST);
    var response = createOrReuseDraftJob(userId, session, jobType);
    var jobId = response.jobId();
    var inputSlot = "unknown_input_slot";

    assertThatExceptionOfType(UnknownOrUnsupportedInputSlotException.class)
        .isThrownBy(() -> jobInputUploadService.issueUploadUrl(userId, jobId, inputSlot))
        .withMessage(
            "Unknown or unsupported input slot '"
                + inputSlot
                + "' for the given jobType 'solving_slae'.");
  }

  private static String newSessionId() {
    return "session-" + UUID.randomUUID();
  }

  private JobCreationResult createOrReuseDraftJob(
      long user, String session, JobType jobType) {
    return jobCreationService.createOrReuseDraftJob(user, session, jobType);
  }
}

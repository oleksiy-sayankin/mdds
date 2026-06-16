/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.server.PresignedUrlAssertions.assertExpiresAtMatchesSignature;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.mdds.domain.JobStatus;
import com.mdds.queue.QueueClient;
import com.mdds.server.support.JobTestFixture;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.MinioException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
@Import(JobTestFixture.class)
class TestJobOutputsServiceIntegration {

  @Autowired private UserLookupService userLookupService;
  @Autowired private JobCreationService jobCreationService;
  @Autowired private JobTestFixture jobFixture;
  @Autowired private JobOutputsService jobOutputsService;

  @MockitoBean(name = "jobQueueClient")
  private QueueClient jobQueueClient;

  private static final String MINIO_BUCKET = "mdds";
  private static final Duration PRESIGNED_GET_TTL = Duration.ofMinutes(15);

  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17")
          .withDatabaseName("mdds")
          .withUsername("mdds")
          .withPassword("mdds123");

  @Container
  private static final MinIOContainer MINIO =
      new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z")
          .withUserName("testuser")
          .withPassword("testpassword");

  @DynamicPropertySource
  static void initProps(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("mdds.object-storage.bucket", () -> MINIO_BUCKET);
    registry.add("mdds.object-storage.region", () -> "us-east-1");
    registry.add("mdds.object-storage.public-endpoint", MINIO::getS3URL);
    registry.add("mdds.object-storage.internal-endpoint", MINIO::getS3URL);
    registry.add("mdds.object-storage.access-key", MINIO::getUserName);
    registry.add("mdds.object-storage.secret-key", MINIO::getPassword);
    registry.add("mdds.object-storage.path-style-access-enabled", () -> "true");
    registry.add("mdds.object-storage.presign-get-ttl", PRESIGNED_GET_TTL::toString);
  }

  private static final String GUEST = "guest";
  private static final String ADMIN = "admin";

  private static MinioClient minioClient;

  @BeforeAll
  static void init() throws MinioException {
    initMinioClient();
    initMinioData();
  }

  private static Stream<String> userLoginValues() {
    return Stream.of(GUEST, ADMIN);
  }

  @ParameterizedTest
  @MethodSource("userLoginValues")
  void testJobOutput(String login) throws IOException, MinioException, URISyntaxException {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(login);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, sessionId, jobType).jobId();
    uploadToMinIO(solutionObjectKey(userId, jobId), "solution.csv");
    jobFixture.forceStatus(jobId, JobStatus.DONE);
    var result = jobOutputsService.issueDownloadUrl(userId, jobId, "solution");
    var downloadUrl = result.downloadUrl();
    var expiresAt = result.expiresAt();
    assertThat(downloadUrl).isNotNull();
    assertThat(expiresAt).isNotNull();
    assertExpiresAtMatchesSignature(result.expiresAt(), result.downloadUrl(), PRESIGNED_GET_TTL);
    var objectKey = extractObjectKeyFromPresignedUrl(downloadUrl);
    assertThat(objectKey).isEqualTo(solutionObjectKey(userId, jobId));

    try (var expected = asStreamFromResources("solution.csv");
        var actual = downloadUrl.openStream()) {
      assertThat(actual).hasSameContentAs(expected);
    }
  }

  private static Stream<JobStatus> invalidJobStatusValues() {
    return Stream.of(
        JobStatus.DRAFT,
        JobStatus.SUBMITTED,
        JobStatus.IN_PROGRESS,
        JobStatus.CANCELLED,
        JobStatus.ERROR,
        JobStatus.VALIDATION_FAILED,
        JobStatus.CANCEL_REQUESTED);
  }

  @ParameterizedTest
  @MethodSource("invalidJobStatusValues")
  void testInvalidJobStatus(JobStatus status) throws IOException, MinioException {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, sessionId, jobType).jobId();
    uploadToMinIO(solutionObjectKey(userId, jobId), "solution.csv");
    jobFixture.forceStatus(jobId, status);

    assertThatExceptionOfType(JobIsNotDoneException.class)
        .isThrownBy(() -> jobOutputsService.issueDownloadUrl(userId, jobId, "solution"))
        .withMessage(
            "Job '" + jobId + "' is not in DONE state and no output artifacts can be downloaded.");
  }

  @Test
  void testJobOfOtherUser() {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var adminUserId = userLookupService.findUserId(ADMIN);
    var jobId = jobCreationService.createOrReuseDraftJob(adminUserId, sessionId, jobType).jobId();
    jobFixture.forceStatus(jobId, JobStatus.DONE);
    var guestUserId = userLookupService.findUserId(GUEST);
    assertThatExceptionOfType(JobDoesNotExistException.class)
        .isThrownBy(() -> jobOutputsService.issueDownloadUrl(guestUserId, jobId, "solution"))
        .withMessage("Job with id '" + jobId + "' does not exist.");
  }

  @Test
  void testInvalidJobId() {
    var userId = userLookupService.findUserId(GUEST);
    var jobId = "invalid-job-id";
    assertThatExceptionOfType(JobDoesNotExistException.class)
        .isThrownBy(() -> jobOutputsService.issueDownloadUrl(userId, jobId, "solution"))
        .withMessage("Job with id '" + jobId + "' does not exist.");
  }

  private static Stream<String> nullOrBlankOutputSlotValues() {
    return Stream.of(null, "", " ");
  }

  @ParameterizedTest
  @MethodSource("nullOrBlankOutputSlotValues")
  void testOutputSlotIsNullOrBlank(String outputSlot) {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, sessionId, jobType).jobId();
    jobFixture.forceStatus(jobId, JobStatus.DONE);
    assertThatExceptionOfType(OutputSlotIsNullOrBlankException.class)
        .isThrownBy(() -> jobOutputsService.issueDownloadUrl(userId, jobId, outputSlot))
        .withMessage("Output slot is null or blank.");
  }

  private static Stream<String> unsupportedOutputSlotValues() {
    return Stream.of("wrong_output_slot", "UNSUPPORTED_OUTPUT_SLOT", "invalid_output_slot");
  }

  @ParameterizedTest
  @MethodSource("unsupportedOutputSlotValues")
  void testOutputSlotIsUnsupported(String outputSlot) {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, sessionId, jobType).jobId();
    jobFixture.forceStatus(jobId, JobStatus.DONE);
    assertThatExceptionOfType(UnknownOrUnsupportedOutputSlotException.class)
        .isThrownBy(() -> jobOutputsService.issueDownloadUrl(userId, jobId, outputSlot))
        .withMessage(
            "Unknown or unsupported output slot '"
                + outputSlot.toLowerCase(Locale.ROOT)
                + "' for the given jobType '"
                + jobType
                + "'.");
  }

  @Test
  void testNoOutputArtifacts() {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, sessionId, jobType).jobId();
    jobFixture.forceStatus(jobId, JobStatus.DONE);
    assertThatExceptionOfType(OutputArtifactDoesNotExistException.class)
        .isThrownBy(() -> jobOutputsService.issueDownloadUrl(userId, jobId, "solution"))
        .withMessage("Output artifact does not exist.");
  }

  private static Stream<String> normalizedOutputSlotValues() {
    return Stream.of("solution", " SOLUTION ", " Solution ");
  }

  @ParameterizedTest
  @MethodSource("normalizedOutputSlotValues")
  void testNormalizedOutputSlotValues(String outputSlot)
      throws IOException, MinioException, URISyntaxException {
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, sessionId, jobType).jobId();
    uploadToMinIO(solutionObjectKey(userId, jobId), "solution.csv");
    jobFixture.forceStatus(jobId, JobStatus.DONE);
    var result = jobOutputsService.issueDownloadUrl(userId, jobId, outputSlot);
    var downloadUrl = result.downloadUrl();
    var expiresAt = result.expiresAt();
    assertThat(downloadUrl).isNotNull();
    assertThat(expiresAt).isNotNull();
    assertExpiresAtMatchesSignature(result.expiresAt(), result.downloadUrl(), PRESIGNED_GET_TTL);
    var objectKey = extractObjectKeyFromPresignedUrl(downloadUrl);
    assertThat(objectKey).isEqualTo(solutionObjectKey(userId, jobId));

    try (var expected = asStreamFromResources("solution.csv");
        var actual = downloadUrl.openStream()) {
      assertThat(actual).hasSameContentAs(expected);
    }
  }

  private static String extractObjectKeyFromPresignedUrl(URL downloadUrl) {
    var path = downloadUrl.getPath();
    var prefix = "/" + MINIO_BUCKET + "/";
    assertThat(path).startsWith(prefix);
    return path.substring(prefix.length());
  }

  private static String newSessionId() {
    return "session-" + UUID.randomUUID();
  }

  private static void initMinioClient() {
    minioClient =
        MinioClient.builder()
            .endpoint(MINIO.getS3URL())
            .credentials(MINIO.getUserName(), MINIO.getPassword())
            .build();
  }

  private static void initMinioData() throws MinioException {
    minioClient.makeBucket(MakeBucketArgs.builder().bucket(MINIO_BUCKET).build());
  }

  private static String solutionObjectKey(long userId, String jobId) {
    return "jobs/" + userId + "/" + jobId + "/out/solution.csv";
  }

  private static void uploadToMinIO(String objectKey, String fileName)
      throws IOException, MinioException {
    try (var is = asStreamFromResources(fileName)) {
      minioClient.putObject(
          PutObjectArgs.builder().bucket(MINIO_BUCKET).object(objectKey).stream(is, -1L, 10485760L)
              .build());
    }
  }

  private static InputStream asStreamFromResources(String fileName) {
    var is = TestJobOutputsServiceIntegration.class.getClassLoader().getResourceAsStream(fileName);
    assertThat(is).isNotNull();
    return is;
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.mdds.common.util.HttpTestClient;
import com.mdds.common.util.JsonHelper;
import com.mdds.domain.JobStatus;
import com.mdds.dto.CreateJobRequestDTO;
import com.mdds.dto.ErrorResponseDTO;
import com.mdds.dto.JobIdResponseDTO;
import com.mdds.dto.JobOutputResponseDTO;
import com.mdds.server.support.JobTestFixture;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.MinioException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Slf4j
@SpringBootTest(
    classes = ServerApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = "mdds.job-profile.mode=yaml")
@Testcontainers
@Import(JobTestFixture.class)
class TestJobOutputsRestApiIntegration {

  @LocalServerPort private int port;

  private static final String HOST = "localhost";
  @Autowired private UserLookupService userLookupService;
  @Autowired private JobTestFixture jobFixture;

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
  void testJobOutput(String login) throws IOException, MinioException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(login);
    var createJobResponse = createOrReuseJob(http, login, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    uploadToMinIO(solutionObjectKey(userId, jobId), "solution.csv");
    jobFixture.forceStatus(jobId, JobStatus.DONE);
    var result = outputs(http, login, jobId, "solution");
    var downloadUrl = result.downloadUrl();
    var expiresAt = result.expiresAt();
    assertThat(downloadUrl).isNotNull();
    assertThat(expiresAt).isNotNull();
    var now = Instant.now();
    assertThat(expiresAt).isAfter(now).isBefore(now.plus(PRESIGNED_GET_TTL));
    var objectKey = extractObjectKeyFromPresignedUrl(downloadUrl);
    assertThat(objectKey).isEqualTo(solutionObjectKey(userId, jobId));

    try (var expected = asStreamFromResources("solution.csv");
        var actual = asStreamFromUrl(downloadUrl)) {
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
  void testInvalidJobStatus(JobStatus status)
      throws IOException, MinioException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    uploadToMinIO(solutionObjectKey(userId, jobId), "solution.csv");
    jobFixture.forceStatus(jobId, status);

    var response =
        http.get(
            "/jobs/" + jobId + "/outputs?outputSlot=solution",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.CONFLICT.value());
    assertThat(message(response.body()))
        .isEqualTo(
            "Job '" + jobId + "' is not in DONE state and no output artifacts can be downloaded.");
  }

  @Test
  void testJobOfOtherUser() throws IOException, InterruptedException, MinioException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(ADMIN);
    var createJobResponse = createOrReuseJob(http, ADMIN, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    uploadToMinIO(solutionObjectKey(userId, jobId), "solution.csv");
    jobFixture.forceStatus(jobId, JobStatus.DONE);

    var response =
        http.get(
            "/jobs/" + jobId + "/outputs?outputSlot=solution",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.NOT_FOUND.value());
    assertThat(message(response.body())).isEqualTo("Job with id '" + jobId + "' does not exist.");
  }

  @Test
  void testInvalidJobId() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var jobId = "invalid-job-id";
    var response =
        http.get(
            "/jobs/" + jobId + "/outputs?outputSlot=solution",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.NOT_FOUND.value());
    assertThat(message(response.body())).isEqualTo("Job with id '" + jobId + "' does not exist.");
  }

  @Test
  void testMissingOutputSlot() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();

    var response =
        http.get(
            "/jobs/" + jobId + "/outputs",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(message(response.body()))
        .isEqualTo("Required request parameter 'outputSlot' is missing.");
  }

  @Test
  void testOutputSlotIsNullOrBlank() throws IOException, InterruptedException, MinioException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    uploadToMinIO(solutionObjectKey(userId, jobId), "solution.csv");
    jobFixture.forceStatus(jobId, JobStatus.DONE);

    var response =
        http.get(
            "/jobs/" + jobId + "/outputs?outputSlot=",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(message(response.body())).isEqualTo("Output slot is null or blank.");
  }

  private static Stream<String> unsupportedOutputSlotValues() {
    return Stream.of("wrong_output_slot", "UNSUPPORTED_OUTPUT_SLOT", "invalid_output_slot");
  }

  @ParameterizedTest
  @MethodSource("unsupportedOutputSlotValues")
  void testOutputSlotIsUnsupported(String outputSlot)
      throws IOException, InterruptedException, MinioException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    uploadToMinIO(solutionObjectKey(userId, jobId), "solution.csv");
    jobFixture.forceStatus(jobId, JobStatus.DONE);

    var response =
        http.get(
            "/jobs/" + jobId + "/outputs?outputSlot=" + outputSlot,
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(message(response.body()))
        .isEqualTo(
            "Unknown or unsupported output slot '"
                + outputSlot.toLowerCase(Locale.ROOT)
                + "' for the given jobType '"
                + jobType
                + "'.");
  }

  @Test
  void testNoOutputArtifacts() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    jobFixture.forceStatus(jobId, JobStatus.DONE);

    var response =
        http.get(
            "/jobs/" + jobId + "/outputs?outputSlot=solution",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
    assertThat(message(response.body())).isEqualTo("Internal Server Error");
  }

  private static Stream<String> normalizedOutputSlotValues() {
    return Stream.of("solution", "SOLUTION", "Solution");
  }

  @ParameterizedTest
  @MethodSource("normalizedOutputSlotValues")
  void testNormalizedOutputSlotValues(String outputSlot)
      throws IOException, MinioException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    uploadToMinIO(solutionObjectKey(userId, jobId), "solution.csv");
    jobFixture.forceStatus(jobId, JobStatus.DONE);
    var result = outputs(http, GUEST, jobId, outputSlot);
    var downloadUrl = result.downloadUrl();
    var expiresAt = result.expiresAt();
    assertThat(downloadUrl).isNotNull();
    assertThat(expiresAt).isNotNull();
    var now = Instant.now();
    assertThat(expiresAt).isAfter(now).isBefore(now.plus(PRESIGNED_GET_TTL));
    var objectKey = extractObjectKeyFromPresignedUrl(downloadUrl);
    assertThat(objectKey).isEqualTo(solutionObjectKey(userId, jobId));

    try (var expected = asStreamFromResources("solution.csv");
        var actual = asStreamFromUrl(downloadUrl)) {
      assertThat(actual).hasSameContentAs(expected);
    }
  }

  @Test
  void testErrorForMissingUser() throws IOException, InterruptedException, MinioException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    uploadToMinIO(solutionObjectKey(userId, jobId), "solution.csv");
    jobFixture.forceStatus(jobId, JobStatus.DONE);

    var response = http.get("/jobs/" + jobId + "/outputs?outputSlot=solution", Map.of());

    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(message(response.body()))
        .isEqualTo("Required request header 'X-MDDS-User-Login' is missing.");
  }

  @Test
  void testOkForMissingContentType() throws IOException, InterruptedException, MinioException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    uploadToMinIO(solutionObjectKey(userId, jobId), "solution.csv");
    jobFixture.forceStatus(jobId, JobStatus.DONE);

    var response =
        http.get(
            "/jobs/" + jobId + "/outputs?outputSlot=solution", Map.of("X-MDDS-User-Login", GUEST));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.OK.value());
    var dto = JsonHelper.fromJson(response.body(), JobOutputResponseDTO.class);
    assertThat(dto).isNotNull();
    assertThat(dto.jobId()).isNotBlank();
    assertThat(dto.downloadUrl()).isNotBlank();
    try (var expected = asStreamFromResources("solution.csv");
        var actual = asStreamFromUrl(dto.downloadUrl())) {
      assertThat(actual).hasSameContentAs(expected);
    }
  }

  private static Stream<Arguments> userValues() {
    return Stream.of(
        Arguments.of(
            "invalid_user", HttpStatus.UNAUTHORIZED.value(), "Unknown user login: invalid_user."),
        Arguments.of("", HttpStatus.BAD_REQUEST.value(), "User is null or blank."),
        Arguments.of("   ", HttpStatus.BAD_REQUEST.value(), "User is null or blank."),
        Arguments.of(" ", HttpStatus.BAD_REQUEST.value(), "User is null or blank."));
  }

  @ParameterizedTest
  @MethodSource("userValues")
  void testInvalidUser(String user, int statusCode, String message)
      throws IOException, InterruptedException, MinioException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    uploadToMinIO(solutionObjectKey(userId, jobId), "solution.csv");
    jobFixture.forceStatus(jobId, JobStatus.DONE);

    var response =
        http.get(
            "/jobs/" + jobId + "/outputs?outputSlot=solution",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", user));

    assertThat(response.statusCode()).isEqualTo(statusCode);
    assertThat(message(response.body())).isEqualTo(message);
  }

  private static String extractObjectKeyFromPresignedUrl(String downloadUrl) {
    var path = URI.create(downloadUrl).getPath();
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
    var is = TestJobOutputsRestApiIntegration.class.getClassLoader().getResourceAsStream(fileName);
    assertThat(is).isNotNull();
    return is;
  }

  private static InputStream asStreamFromUrl(String downloadUrl) throws IOException {
    var uri = URI.create(downloadUrl);
    return uri.toURL().openStream();
  }

  private static JobIdResponseDTO createOrReuseJob(
      HttpTestClient http, String userLogin, String uploadSessionId, String jobType)
      throws IOException, InterruptedException {
    var response =
        http.post(
            "/jobs",
            Map.of(
                "Content-Type",
                "application/json",
                "X-MDDS-User-Login",
                userLogin,
                "X-MDDS-Upload-Session-Id",
                uploadSessionId),
            new CreateJobRequestDTO(jobType));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.CREATED.value());

    var dto = JsonHelper.fromJson(response.body(), JobIdResponseDTO.class);
    assertThat(dto).isNotNull();
    assertThat(dto.getJobId()).isNotBlank();
    return dto;
  }

  private static JobOutputResponseDTO outputs(
      HttpTestClient http, String userLogin, String jobId, String outputSlot)
      throws IOException, InterruptedException {
    var response =
        http.get(
            "/jobs/" + jobId + "/outputs?outputSlot=" + outputSlot,
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", userLogin));
    assertThat(response.statusCode()).isEqualTo(HttpStatus.OK.value());
    var dto = JsonHelper.fromJson(response.body(), JobOutputResponseDTO.class);
    assertThat(dto).isNotNull();
    assertThat(dto.jobId()).isNotBlank();
    assertThat(dto.downloadUrl()).isNotBlank();
    return dto;
  }

  private static String message(String rawJson) {
    return JsonHelper.fromJson(rawJson, ErrorResponseDTO.class).message();
  }
}

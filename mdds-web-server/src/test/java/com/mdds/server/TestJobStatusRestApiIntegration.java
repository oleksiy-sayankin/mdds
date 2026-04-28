/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdds.common.util.HttpTestClient;
import com.mdds.common.util.JsonHelper;
import com.mdds.domain.JobStatus;
import com.mdds.dto.CreateJobRequestDTO;
import com.mdds.dto.ErrorResponseDTO;
import com.mdds.dto.JobIdResponseDTO;
import com.mdds.dto.JobStatusResponseDTO;
import com.mdds.dto.JobSubmitResponseDTO;
import com.mdds.dto.JobUploadUrlRequestDTO;
import com.mdds.dto.JobUploadUrlResponseDTO;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.errors.MinioException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
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
class TestJobStatusRestApiIntegration {

  @LocalServerPort private int port;

  private static final String HOST = "localhost";
  private static final String GUEST = "guest";
  private static final String ADMIN = "admin";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String MINIO_USER = "testuser";
  private static final String MINIO_PASSWORD = "testpassword";
  private static final String MINIO_BUCKET = "mdds";
  private static final Duration PRE_SIGNED_PUT_TTL = Duration.ofMinutes(15);

  private static MinioClient minioClient;

  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17")
          .withDatabaseName("mdds")
          .withUsername("mdds")
          .withPassword("mdds123");

  @Container
  private static final RabbitMQContainer rabbitMq =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672)
          .waitingFor(Wait.forListeningPort())
          .withStartupTimeout(Duration.ofSeconds(30));

  @Container
  private static final MinIOContainer MINIO =
      new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z")
          .withUserName(MINIO_USER)
          .withPassword(MINIO_PASSWORD);

  @DynamicPropertySource
  static void initProps(DynamicPropertyRegistry registry) {
    registry.add("mdds.rabbitmq.host", rabbitMq::getHost);
    registry.add("mdds.rabbitmq.port", rabbitMq::getAmqpPort);
    registry.add("mdds.rabbitmq.user", rabbitMq::getAdminUsername);
    registry.add("mdds.rabbitmq.password", rabbitMq::getAdminPassword);
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("mdds.object-storage.public-endpoint", MINIO::getS3URL);
    registry.add("mdds.object-storage.internal-endpoint", MINIO::getS3URL);
    registry.add("mdds.object-storage.access-key", MINIO::getUserName);
    registry.add("mdds.object-storage.secret-key", MINIO::getPassword);
    registry.add("mdds.object-storage.path-style-access-enabled", () -> "true");
    registry.add("mdds.object-storage.presign-put-ttl", PRE_SIGNED_PUT_TTL::toString);
  }

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
  void testSubmissionJobStatus(String login)
      throws IOException, URISyntaxException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var beforeCreation = Instant.now();
    var createJobResponse = createOrReuseJob(http, login, sessionId, jobType);
    var afterCreation = Instant.now();
    var jobId = createJobResponse.getJobId();

    var response = status(http, login, jobId);
    assertThat(response.status()).isEqualTo(JobStatus.DRAFT.toString());

    var inputSlot = "matrix";
    var issueUrlResponse = issueUploadUrl(http, login, jobId, inputSlot);
    var url = issueUrlResponse.uploadUrl();
    var source = getPathFromResources("matrix.csv");
    upload(http, url, source);

    inputSlot = "rhs";
    issueUrlResponse = issueUploadUrl(http, login, jobId, inputSlot);
    url = issueUrlResponse.uploadUrl();
    source = getPathFromResources("rhs.csv");
    upload(http, url, source);

    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var paramsAsMap = Map.of(paramName, paramValue);
    var paramsAsJson = JsonHelper.toJson(paramsAsMap);
    patchParams(http, login, jobId, paramsAsJson);

    var beforeSubmission = Instant.now();
    var result = submit(http, login, jobId);
    var afterSubmission = Instant.now();
    assertThat(result.jobId()).isEqualTo(jobId);
    assertThat(result.status()).isEqualTo(JobStatus.SUBMITTED.toString());

    response = status(http, login, jobId);
    assertThat(response.jobId()).isEqualTo(jobId);
    assertThat(response.jobType()).isEqualTo(jobType);
    assertThat(response.status()).isEqualTo(JobStatus.SUBMITTED.toString());
    assertThat(response.progress()).isZero();
    assertThat(response.message()).isNull();
    assertThat(response.submittedAt()).isStrictlyBetween(beforeSubmission, afterSubmission);
    assertThat(response.createdAt()).isStrictlyBetween(beforeCreation, afterCreation);
    assertThat(response.startedAt()).isNull();
    assertThat(response.finishedAt()).isNull();
  }

  @Test
  void testStatusForUnknownJobId() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var jobId = UUID.randomUUID().toString();
    var response = http.get("/jobs/" + jobId + "/status", Map.of("X-MDDS-User-Login", GUEST));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.NOT_FOUND.value());
    assertThat(message(response.body())).isEqualTo("Job with id '" + jobId + "' does not exist.");
  }

  @Test
  void testStatusNoJob() throws URISyntaxException, IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, ADMIN, sessionId, jobType);
    var jobId = createJobResponse.getJobId();

    var inputSlot = "matrix";
    var issueUrlResponse = issueUploadUrl(http, ADMIN, jobId, inputSlot);
    var url = issueUrlResponse.uploadUrl();
    var source = getPathFromResources("matrix.csv");
    upload(http, url, source);

    inputSlot = "rhs";
    issueUrlResponse = issueUploadUrl(http, ADMIN, jobId, inputSlot);
    url = issueUrlResponse.uploadUrl();
    source = getPathFromResources("rhs.csv");
    upload(http, url, source);

    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var paramsAsMap = Map.of(paramName, paramValue);
    var paramsAsJson = JsonHelper.toJson(paramsAsMap);
    patchParams(http, ADMIN, jobId, paramsAsJson);
    submit(http, ADMIN, jobId);

    var response = http.get("/jobs/" + jobId + "/status", Map.of("X-MDDS-User-Login", GUEST));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.NOT_FOUND.value());
    assertThat(message(response.body())).isEqualTo("Job with id '" + jobId + "' does not exist.");
  }

  @Test
  void testErrorForMissingUser() throws URISyntaxException, IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();

    var inputSlot = "matrix";
    var issueUrlResponse = issueUploadUrl(http, GUEST, jobId, inputSlot);
    var url = issueUrlResponse.uploadUrl();
    var source = getPathFromResources("matrix.csv");
    upload(http, url, source);

    inputSlot = "rhs";
    issueUrlResponse = issueUploadUrl(http, GUEST, jobId, inputSlot);
    url = issueUrlResponse.uploadUrl();
    source = getPathFromResources("rhs.csv");
    upload(http, url, source);

    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var paramsAsMap = Map.of(paramName, paramValue);
    var paramsAsJson = JsonHelper.toJson(paramsAsMap);
    patchParams(http, GUEST, jobId, paramsAsJson);

    submit(http, GUEST, jobId);

    var response =
        http.get("/jobs/" + jobId + "/status", Map.of("Content-Type", "application/json"));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(message(response.body()))
        .isEqualTo("Required request header 'X-MDDS-User-Login' is missing.");
  }

  @Test
  void testOkForMissingContentType() throws URISyntaxException, IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();

    var inputSlot = "matrix";
    var issueUrlResponse = issueUploadUrl(http, GUEST, jobId, inputSlot);
    var url = issueUrlResponse.uploadUrl();
    var source = getPathFromResources("matrix.csv");
    upload(http, url, source);

    inputSlot = "rhs";
    issueUrlResponse = issueUploadUrl(http, GUEST, jobId, inputSlot);
    url = issueUrlResponse.uploadUrl();
    source = getPathFromResources("rhs.csv");
    upload(http, url, source);

    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var paramsAsMap = Map.of(paramName, paramValue);
    var paramsAsJson = JsonHelper.toJson(paramsAsMap);
    patchParams(http, GUEST, jobId, paramsAsJson);

    submit(http, GUEST, jobId);

    var response = http.get("/jobs/" + jobId + "/status", Map.of("X-MDDS-User-Login", GUEST));

    var result = JsonHelper.fromJson(response.body(), JobStatusResponseDTO.class);
    assertThat(result.status()).isEqualTo(JobStatus.SUBMITTED.toString());
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
      throws IOException, InterruptedException, URISyntaxException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();

    var inputSlot = "matrix";
    var issueUrlResponse = issueUploadUrl(http, GUEST, jobId, inputSlot);
    var url = issueUrlResponse.uploadUrl();
    var source = getPathFromResources("matrix.csv");
    upload(http, url, source);

    inputSlot = "rhs";
    issueUrlResponse = issueUploadUrl(http, GUEST, jobId, inputSlot);
    url = issueUrlResponse.uploadUrl();
    source = getPathFromResources("rhs.csv");
    upload(http, url, source);

    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var paramsAsMap = Map.of(paramName, paramValue);
    var paramsAsJson = JsonHelper.toJson(paramsAsMap);
    patchParams(http, GUEST, jobId, paramsAsJson);

    var response = http.get("/jobs/" + jobId + "/status", Map.of("X-MDDS-User-Login", user));

    assertThat(response.statusCode()).isEqualTo(statusCode);
    assertThat(message(response.body())).isEqualTo(message);
  }

  private static void initMinioClient() {
    minioClient =
        MinioClient.builder()
            .endpoint(MINIO.getS3URL())
            .credentials(MINIO_USER, MINIO_PASSWORD)
            .build();
    log.info("MinIO client is ready");
  }

  private static void initMinioData() throws MinioException {
    minioClient.makeBucket(MakeBucketArgs.builder().bucket(MINIO_BUCKET).build());
  }

  private static String newSessionId() {
    return "session-" + UUID.randomUUID();
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

  private static JobUploadUrlResponseDTO issueUploadUrl(
      HttpTestClient http, String userLogin, String jobId, String inputSlot)
      throws IOException, InterruptedException {
    var response =
        http.post(
            "/jobs/" + jobId + "/inputs",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", userLogin),
            new JobUploadUrlRequestDTO(inputSlot));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.OK.value());

    var dto = JsonHelper.fromJson(response.body(), JobUploadUrlResponseDTO.class);
    assertThat(dto).isNotNull();
    assertThat(dto.jobId()).isNotBlank();
    assertThat(dto.uploadUrl()).isNotBlank();
    assertThat(dto.expiresAt()).isNotNull();
    return dto;
  }

  private static JobStatusResponseDTO status(HttpTestClient http, String userLogin, String jobId)
      throws IOException, InterruptedException {
    var response =
        http.get(
            "/jobs/" + jobId + "/status",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", userLogin));

    return JsonHelper.fromJson(response.body(), JobStatusResponseDTO.class);
  }

  private static void upload(HttpTestClient http, String url, Path source)
      throws IOException, InterruptedException {
    var response = http.put(url, source);
    assertThat(response.statusCode()).isEqualTo(HttpStatus.OK.value());
  }

  private static Path getPathFromResources(String fileName) throws URISyntaxException {
    var resourceUrl =
        TestJobInputUploadRestApiIntegration.class.getClassLoader().getResource(fileName);
    assertThat(resourceUrl).isNotNull();
    var resourceUri = resourceUrl.toURI();
    return Paths.get(resourceUri);
  }

  private static void patchParams(
      HttpTestClient http, String userLogin, String jobId, String rawJson)
      throws IOException, InterruptedException {

    var response =
        http.patch(
            "/jobs/" + jobId + "/params",
            Map.of("Content-Type", "application/merge-patch+json", "X-MDDS-User-Login", userLogin),
            rawJson);

    assertThat(response.statusCode()).isEqualTo(HttpStatus.OK.value());
  }

  private static JobSubmitResponseDTO submit(HttpTestClient http, String userLogin, String jobId)
      throws IOException, InterruptedException {
    var response =
        http.post(
            "/jobs/" + jobId + "/submit",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", userLogin));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.ACCEPTED.value());

    return JsonHelper.fromJson(response.body(), JobSubmitResponseDTO.class);
  }

  private static String message(String rawJson) {
    return JsonHelper.fromJson(rawJson, ErrorResponseDTO.class).message();
  }
}

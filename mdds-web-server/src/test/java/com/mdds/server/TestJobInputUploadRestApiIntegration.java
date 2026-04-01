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
import com.mdds.dto.JobUploadUrlRequestDTO;
import com.mdds.dto.JobUploadUrlResponseDTO;
import com.mdds.server.support.JobTestFixture;
import io.minio.GetObjectArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.errors.MinioException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@SpringBootTest(
    classes = ServerApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@Import(JobTestFixture.class)
class TestJobInputUploadRestApiIntegration {

  @LocalServerPort private int port;
  @Autowired private JobTestFixture jobFixture;

  private static final String HOST = "localhost";
  private static final String GUEST = "guest";
  private static final String ADMIN = "admin";
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
  private static final MinIOContainer MINIO =
      new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z")
          .withUserName(MINIO_USER)
          .withPassword(MINIO_PASSWORD);

  @DynamicPropertySource
  static void initProps(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("mdds.object-storage.bucket", () -> MINIO_BUCKET);
    registry.add("mdds.object-storage.region", () -> "us-east-1");
    registry.add("mdds.object-storage.public-endpoint", MINIO::getS3URL);
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
  void testUploadMatrixToObjectStorageUsingPresignedUrl(String userLogin)
      throws IOException, InterruptedException, URISyntaxException, MinioException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, userLogin, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var inputSlot = "matrix";
    var issueUrlResponse = issueUploadUrl(http, userLogin, jobId, inputSlot);
    var url = issueUrlResponse.uploadUrl();
    assertThat(url).isNotBlank();
    var source = getPathFromResources("matrix.csv");
    upload(http, url, source);
    var objectKey = extractObjectKeyFromPresignedUrl(url);
    try (var expected = asStreamFromResources("matrix.csv");
        var actual = asStreamFromMinio(objectKey)) {
      assertThat(actual).hasSameContentAs(expected);
    }
  }

  @Test
  void testArtifactContentIsNotValidatedDuringDirectUpload()
      throws IOException, InterruptedException, URISyntaxException, MinioException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var inputSlot = "matrix";
    var issueUrlResponse = issueUploadUrl(http, GUEST, jobId, inputSlot);
    var url = issueUrlResponse.uploadUrl();
    assertThat(url).isNotBlank();
    var source = getPathFromResources("unsupported-matrix.csv");
    upload(http, url, source);
    var objectKey = extractObjectKeyFromPresignedUrl(url);
    try (var expected = asStreamFromResources("unsupported-matrix.csv");
        var actual = asStreamFromMinio(objectKey)) {
      assertThat(actual).hasSameContentAs(expected);
    }
  }

  private static Stream<String> notNormalizedInputSlots() {
    return Stream.of(" matrix", " matrix ", "MATRIX", "Matrix", "MaTrIX");
  }

  @ParameterizedTest
  @MethodSource("notNormalizedInputSlots")
  void testInputSlotNormalization(String notNormalizedInputSlot)
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var issueUrlResponse = issueUploadUrl(http, GUEST, jobId, notNormalizedInputSlot);
    var url = issueUrlResponse.uploadUrl();
    var objectKey = extractObjectKeyFromPresignedUrl(url);
    assertThat(objectKey).contains("matrix");
  }

  @Test
  void testMultipleUpload()
      throws IOException, InterruptedException, URISyntaxException, MinioException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var inputSlot = "matrix";

    var issueUrlResponse = issueUploadUrl(http, GUEST, jobId, inputSlot);
    var url = issueUrlResponse.uploadUrl();
    assertThat(url).isNotBlank();
    var source = getPathFromResources("matrix.csv");
    upload(http, url, source);
    var objectKey = extractObjectKeyFromPresignedUrl(url);
    try (var expected = asStreamFromResources("matrix.csv");
        var actual = asStreamFromMinio(objectKey)) {
      assertThat(actual).hasSameContentAs(expected);
    }

    issueUrlResponse = issueUploadUrl(http, GUEST, jobId, inputSlot);
    url = issueUrlResponse.uploadUrl();
    assertThat(url).isNotBlank();
    source = getPathFromResources("new-matrix.csv");
    upload(http, url, source);
    objectKey = extractObjectKeyFromPresignedUrl(url);
    try (var expected = asStreamFromResources("new-matrix.csv");
        var actual = asStreamFromMinio(objectKey)) {
      assertThat(actual).hasSameContentAs(expected);
    }
  }

  @Test
  void testValidExpiresAtValue() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var inputSlot = "matrix";

    var before = Instant.now();
    var response = issueUploadUrl(http, GUEST, jobId, inputSlot);
    var after = Instant.now();
    var expiresAt = response.expiresAt();

    assertThat(expiresAt)
        .isAfterOrEqualTo(before.plus(PRE_SIGNED_PUT_TTL).minusSeconds(1))
        .isBeforeOrEqualTo(after.plus(PRE_SIGNED_PUT_TTL).plusSeconds(1));
  }

  private static Stream<Arguments> slots() {
    return Stream.of(Arguments.of("matrix", "matrix.csv"), Arguments.of("rhs", "rhs.csv"));
  }

  @ParameterizedTest
  @MethodSource("slots")
  void testUploadObjectToObjectStorageUsingPresignedUrlAndDifferentSlots(
      String inputSlot, String fileName)
      throws IOException, InterruptedException, URISyntaxException, MinioException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var issueUrlResponse = issueUploadUrl(http, GUEST, jobId, inputSlot);
    var url = issueUrlResponse.uploadUrl();
    assertThat(url).isNotBlank();
    var source = getPathFromResources(fileName);
    upload(http, url, source);
    var objectKey = extractObjectKeyFromPresignedUrl(url);
    try (var expected = asStreamFromResources(fileName);
        var actual = asStreamFromMinio(objectKey)) {
      assertThat(actual).hasSameContentAs(expected);
    }
  }

  @Test
  void testUnknownOrUnsupportedSlot() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var inputSlot = "wrong_slot";
    var response =
        http.post(
            "/jobs/" + jobId + "/inputs",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST),
            new JobUploadUrlRequestDTO(inputSlot));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(message(response.body()))
        .isEqualTo(
            "Unknown or unsupported input slot '"
                + inputSlot
                + "' for the given jobType '"
                + jobType
                + "'.");
  }

  private static Stream<String> inputSlotValues() {
    return Stream.of(null, " ", "", "   ");
  }

  @ParameterizedTest
  @MethodSource("inputSlotValues")
  void testInputSlotIsNullOrBlank(String inputSlot) throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var response =
        http.post(
            "/jobs/" + jobId + "/inputs",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST),
            new JobUploadUrlRequestDTO(inputSlot));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(message(response.body())).isEqualTo("inputSlot: must not be null or blank.");
  }

  private static Stream<Arguments> jsonBodyValues() {
    return Stream.of(
        Arguments.of("", "Request body is missing or malformed."),
        Arguments.of(" ", "Request body is missing or malformed."),
        Arguments.of("   ", "Request body is missing or malformed."),
        Arguments.of("{}", "inputSlot: must not be null or blank."),
        Arguments.of("{inputSlot:::malformed}", "Request body is missing or malformed."));
  }

  @ParameterizedTest
  @MethodSource("jsonBodyValues")
  void testInvalidOrIncompleteRequestBody(String jsonBody, String message)
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var response =
        http.post(
            "/jobs/" + jobId + "/inputs",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST),
            jsonBody);

    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(message(response.body())).isEqualTo(message);
  }

  @Test
  void testErrorForMissingUser() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var inputSlot = "matrix";
    var response =
        http.post(
            "/jobs/" + jobId + "/inputs",
            Map.of("Content-Type", "application/json"),
            new JobUploadUrlRequestDTO(inputSlot));
    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(message(response.body()))
        .isEqualTo("Required request header 'X-MDDS-User-Login' is missing.");
  }

  @Test
  void testErrorForMissingContentType() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var inputSlot = "matrix";
    var response =
        http.post(
            "/jobs/" + jobId + "/inputs",
            Map.of("X-MDDS-User-Login", GUEST),
            new JobUploadUrlRequestDTO(inputSlot));
    assertThat(response.statusCode()).isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE.value());
  }

  @Test
  void testErrorForInvalidContentType() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var inputSlot = "matrix";
    var response =
        http.post(
            "/jobs/" + jobId + "/inputs",
            Map.of("Content-Type", "application/xml", "X-MDDS-User-Login", GUEST),
            new JobUploadUrlRequestDTO(inputSlot));
    assertThat(response.statusCode()).isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE.value());
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
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var response =
        http.post(
            "/jobs/" + jobId + "/inputs",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", user),
            new JobUploadUrlRequestDTO("matrix"));

    assertThat(response.statusCode()).isEqualTo(statusCode);
    assertThat(message(response.body())).isEqualTo(message);
  }

  @Test
  void testJobDoesNotExist() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, ADMIN, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var response =
        http.post(
            "/jobs/" + jobId + "/inputs",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST),
            new JobUploadUrlRequestDTO("matrix"));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.NOT_FOUND.value());
    assertThat(message(response.body())).isEqualTo("Job with id '" + jobId + "' does not exist.");
  }

  @Test
  void testJobIsNotInDraftState() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    jobFixture.forceStatus(jobId, JobStatus.SUBMITTED);
    var response =
        http.post(
            "/jobs/" + jobId + "/inputs",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST),
            new JobUploadUrlRequestDTO("matrix"));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.CONFLICT.value());
    assertThat(message(response.body()))
        .isEqualTo(
            "Job '"
                + jobId
                + "' is not in DRAFT state and no more input artifacts can be uploaded.");
  }

  private static void upload(HttpTestClient http, String url, Path source)
      throws IOException, InterruptedException {
    var response = http.put(url, source);
    assertThat(response.statusCode()).isEqualTo(HttpStatus.OK.value());
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

  private static Path getPathFromResources(String fileName) throws URISyntaxException {
    var resourceUrl =
        TestJobInputUploadRestApiIntegration.class.getClassLoader().getResource(fileName);
    assertThat(resourceUrl).isNotNull();
    var resourceUri = resourceUrl.toURI();
    return Paths.get(resourceUri);
  }

  private static String newSessionId() {
    return "session-" + UUID.randomUUID();
  }

  private static InputStream asStreamFromMinio(String objectKey) throws MinioException {
    return minioClient.getObject(
        GetObjectArgs.builder().bucket(MINIO_BUCKET).object(objectKey).build());
  }

  private static InputStream asStreamFromResources(String fileName) {
    var is =
        TestJobInputUploadRestApiIntegration.class.getClassLoader().getResourceAsStream(fileName);
    assertThat(is).isNotNull();
    return is;
  }

  private static void initMinioClient() {
    minioClient =
        MinioClient.builder()
            .endpoint(MINIO.getS3URL())
            .credentials(MINIO_USER, MINIO_PASSWORD)
            .build();
    log.info("MinIO client is ready");
  }

  private static String extractObjectKeyFromPresignedUrl(String uploadUrl) {
    var path = URI.create(uploadUrl).getPath();
    var prefix = "/" + MINIO_BUCKET + "/";
    assertThat(path).startsWith(prefix);
    return path.substring(prefix.length());
  }

  private static void initMinioData() throws MinioException {
    minioClient.makeBucket(MakeBucketArgs.builder().bucket(MINIO_BUCKET).build());
  }

  private static String message(String rawJson) {
    return JsonHelper.fromJson(rawJson, ErrorResponseDTO.class).message();
  }
}

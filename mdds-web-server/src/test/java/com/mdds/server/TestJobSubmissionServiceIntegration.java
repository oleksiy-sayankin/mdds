/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdds.domain.JobStatus;
import com.mdds.queue.Queue;
import com.mdds.server.jpa.JobsRepository;
import com.mdds.server.support.JobTestFixture;
import io.minio.GetObjectArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import io.minio.errors.MinioException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
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
class TestJobSubmissionServiceIntegration {

  @Autowired private JobSubmissionService jobSubmissionService;
  @Autowired private UserLookupService userLookupService;
  @Autowired private JobCreationService jobCreationService;
  @Autowired private JobParamsService jobParamsService;
  @Autowired private JobInputUploadService jobInputUploadService;
  @Autowired private JobsRepository jobsRepository;
  @Autowired private JobTestFixture jobFixture;

  @MockitoBean(name = "jobQueue")
  private Queue jobQueue;

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String MINIO_BUCKET = "mdds";
  private static final Duration PRE_SIGNED_PUT_TTL = Duration.ofMinutes(15);

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
    registry.add("mdds.object-storage.presign-put-ttl", PRE_SIGNED_PUT_TTL::toString);
  }

  private static final String GUEST = "guest";
  private static final String ADMIN = "admin";

  private static Stream<String> userLoginValues() {
    return Stream.of(GUEST, ADMIN);
  }

  private static MinioClient minioClient;

  @BeforeAll
  static void init() throws MinioException {
    initMinioClient();
    initMinioData();
  }

  @ParameterizedTest
  @MethodSource("userLoginValues")
  void testSubmissionJobStatus(String login)
      throws IOException, URISyntaxException, MinioException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(login);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, session, jobType).jobId();

    var result = jobInputUploadService.issueUploadUrl(userId, jobId, "matrix");
    var matrixKey = extractObjectKeyFromPresignedUrl(result.uploadUrl());
    upload(matrixKey, "matrix.csv");

    result = jobInputUploadService.issueUploadUrl(userId, jobId, "rhs");
    var rhsKey = extractObjectKeyFromPresignedUrl(result.uploadUrl());
    upload(rhsKey, "rhs.csv");

    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var params = Map.of(paramName, paramValue);
    jobParamsService.mergeParams(userId, jobId, params);

    jobSubmissionService.submit(userId, jobId);
    var job = jobsRepository.findById(jobId).orElseThrow();
    assertThat(job.getStatus()).isEqualTo(JobStatus.SUBMITTED);
    var queueName = "queue-" + jobType;
    verify(jobQueue).publish(eq(queueName), any());
  }

  @ParameterizedTest
  @MethodSource("userLoginValues")
  void testSubmissionManifest(String login) throws IOException, URISyntaxException, MinioException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(login);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, session, jobType).jobId();

    var result = jobInputUploadService.issueUploadUrl(userId, jobId, "matrix");
    var matrixKey = extractObjectKeyFromPresignedUrl(result.uploadUrl());
    upload(matrixKey, "matrix.csv");

    result = jobInputUploadService.issueUploadUrl(userId, jobId, "rhs");
    var rhsKey = extractObjectKeyFromPresignedUrl(result.uploadUrl());
    upload(rhsKey, "rhs.csv");

    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var params = Map.of(paramName, paramValue);
    jobParamsService.mergeParams(userId, jobId, params);
    jobSubmissionService.submit(userId, jobId);

    var manifestObjectKey = manifestObjectKey(userId, jobId);
    var manifest = read(manifestObjectKey);
    assertThat(manifest).isNotBlank();
    var jsonNode = MAPPER.readTree(manifest);

    var solutionKey = solutionObjectKey(userId, jobId);
    assertThat(jsonNode.get("manifestVersion").asInt()).isEqualTo(1);
    assertThat(jsonNode.get("userId").asInt()).isEqualTo(userId);
    assertThat(jsonNode.get("jobId").asText()).isEqualTo(jobId);
    assertThat(jsonNode.get("jobType").asText()).isEqualTo(jobType);
    assertThat(jsonNode.get("inputs").get("matrix").get("objectKey").asText()).isEqualTo(matrixKey);
    assertThat(jsonNode.get("inputs").get("matrix").get("format").asText()).isEqualTo("csv");
    assertThat(jsonNode.get("inputs").get("rhs").get("objectKey").asText()).isEqualTo(rhsKey);
    assertThat(jsonNode.get("inputs").get("rhs").get("format").asText()).isEqualTo("csv");
    assertThat(jsonNode.get("params").get("solvingMethod").asText())
        .isEqualTo("numpy_exact_solver");
    assertThat(jsonNode.get("outputs").get("solution").get("objectKey").asText())
        .isEqualTo(solutionKey);
    assertThat(jsonNode.get("outputs").get("solution").get("format").asText()).isEqualTo("csv");

    var queueName = "queue-" + jobType;
    verify(jobQueue).publish(eq(queueName), any());
  }

  @Test
  void testSubmissionNoRhs() throws URISyntaxException, JsonProcessingException, MinioException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, session, jobType).jobId();

    var result = jobInputUploadService.issueUploadUrl(userId, jobId, "matrix");
    var key = extractObjectKeyFromPresignedUrl(result.uploadUrl());
    upload(key, "matrix.csv");

    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var params = Map.of(paramName, paramValue);
    jobParamsService.mergeParams(userId, jobId, params);

    assertThatExceptionOfType(RequiredInputArtifactIsAbsentException.class)
        .isThrownBy(() -> jobSubmissionService.submit(userId, jobId))
        .withMessage("Required input artifact 'rhs.csv' is absent in object storage.");
    verifyNoInteractions(jobQueue);
  }

  @Test
  void testSubmissionNoMatrix() throws URISyntaxException, JsonProcessingException, MinioException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, session, jobType).jobId();

    var result = jobInputUploadService.issueUploadUrl(userId, jobId, "rhs");
    var key = extractObjectKeyFromPresignedUrl(result.uploadUrl());
    upload(key, "rhs.csv");

    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var params = Map.of(paramName, paramValue);
    jobParamsService.mergeParams(userId, jobId, params);

    assertThatExceptionOfType(RequiredInputArtifactIsAbsentException.class)
        .isThrownBy(() -> jobSubmissionService.submit(userId, jobId))
        .withMessage("Required input artifact 'matrix.csv' is absent in object storage.");
    verifyNoInteractions(jobQueue);
  }

  @Test
  void testSubmissionNoParams() throws URISyntaxException, MinioException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, session, jobType).jobId();

    var result = jobInputUploadService.issueUploadUrl(userId, jobId, "matrix");
    var key = extractObjectKeyFromPresignedUrl(result.uploadUrl());
    upload(key, "matrix.csv");

    result = jobInputUploadService.issueUploadUrl(userId, jobId, "rhs");
    key = extractObjectKeyFromPresignedUrl(result.uploadUrl());
    upload(key, "rhs.csv");

    assertThatExceptionOfType(RequiredParameterIsAbsentException.class)
        .isThrownBy(() -> jobSubmissionService.submit(userId, jobId))
        .withMessage("Required parameter 'solvingMethod' is absent.");
    verifyNoInteractions(jobQueue);
  }

  @Test
  void testSubmissionNotInDraft()
      throws URISyntaxException, JsonProcessingException, MinioException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var jobId = jobCreationService.createOrReuseDraftJob(userId, session, jobType).jobId();

    var result = jobInputUploadService.issueUploadUrl(userId, jobId, "matrix");
    var key = extractObjectKeyFromPresignedUrl(result.uploadUrl());
    upload(key, "matrix.csv");

    result = jobInputUploadService.issueUploadUrl(userId, jobId, "rhs");
    key = extractObjectKeyFromPresignedUrl(result.uploadUrl());
    upload(key, "rhs.csv");

    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var params = Map.of(paramName, paramValue);
    jobParamsService.mergeParams(userId, jobId, params);

    jobFixture.forceStatus(jobId, JobStatus.SUBMITTED);

    assertThatExceptionOfType(JobIsNotDraftException.class)
        .isThrownBy(() -> jobSubmissionService.submit(userId, jobId))
        .withMessage("Job '" + jobId + "' is not in DRAFT state and submission is not allowed.");
    verifyNoInteractions(jobQueue);
  }

  @Test
  void testSubmissionNoJob() throws URISyntaxException, JsonProcessingException, MinioException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var adminUserId = userLookupService.findUserId(ADMIN);
    var jobId = jobCreationService.createOrReuseDraftJob(adminUserId, session, jobType).jobId();

    var result = jobInputUploadService.issueUploadUrl(adminUserId, jobId, "matrix");
    var key = extractObjectKeyFromPresignedUrl(result.uploadUrl());
    upload(key, "matrix.csv");

    result = jobInputUploadService.issueUploadUrl(adminUserId, jobId, "rhs");
    key = extractObjectKeyFromPresignedUrl(result.uploadUrl());
    upload(key, "rhs.csv");

    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var params = Map.of(paramName, paramValue);
    jobParamsService.mergeParams(adminUserId, jobId, params);

    var guestUserId = userLookupService.findUserId(GUEST);

    assertThatExceptionOfType(JobDoesNotExistException.class)
        .isThrownBy(() -> jobSubmissionService.submit(guestUserId, jobId))
        .withMessage("Job with id '" + jobId + "' does not exist.");
    verifyNoInteractions(jobQueue);
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

  private static void upload(String key, String file) throws URISyntaxException, MinioException {
    minioClient.uploadObject(
        UploadObjectArgs.builder()
            .bucket(MINIO_BUCKET)
            .object(key)
            .filename(getPathFromResources(file).toString())
            .build());
  }

  private static Path getPathFromResources(String fileName) throws URISyntaxException {
    var resourceUrl =
        TestJobSubmissionServiceIntegration.class.getClassLoader().getResource(fileName);
    assertThat(resourceUrl).isNotNull();
    var resourceUri = resourceUrl.toURI();
    return Paths.get(resourceUri);
  }

  private static String read(String key) throws MinioException {
    try (var stream =
        minioClient.getObject(GetObjectArgs.builder().bucket(MINIO_BUCKET).object(key).build())) {

      return new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
          .lines()
          .collect(Collectors.joining("\n"));
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static String extractObjectKeyFromPresignedUrl(URL uploadUrl) {
    var path = uploadUrl.getPath();
    var prefix = "/" + MINIO_BUCKET + "/";
    assertThat(path).startsWith(prefix);
    return path.substring(prefix.length());
  }

  private static String newSessionId() {
    return "session-" + UUID.randomUUID();
  }

  private static String manifestObjectKey(long userId, String jobId) {
    return "jobs/" + userId + "/" + jobId + "/manifest.json";
  }

  private static String solutionObjectKey(long userId, String jobId) {
    return "jobs/" + userId + "/" + jobId + "/out/solution.csv";
  }
}

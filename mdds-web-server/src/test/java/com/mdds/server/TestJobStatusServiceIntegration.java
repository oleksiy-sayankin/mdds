/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdds.domain.JobStatus;
import com.mdds.queue.Queue;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import io.minio.errors.MinioException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(properties = {"spring.config.import=classpath:test-job-profiles.yml"})
@Testcontainers
class TestJobStatusServiceIntegration {
  @Autowired private JobSubmissionService jobSubmissionService;
  @Autowired private UserLookupService userLookupService;
  @Autowired private JobCreationService jobCreationService;
  @Autowired private JobParamsService jobParamsService;
  @Autowired private JobInputUploadService jobInputUploadService;
  @Autowired private JobStatusService jobStatusService;

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
  void testJobStatus(String login) throws IOException, URISyntaxException, MinioException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(login);
    var beforeCreation = Instant.now();
    var jobId = jobCreationService.createOrReuseDraftJob(userId, session, jobType).jobId();
    var afterCreation = Instant.now();

    var statusResponse = jobStatusService.status(userId, jobId);
    assertThat(statusResponse.status()).isEqualTo(JobStatus.DRAFT.toString());

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

    var beforeSubmission = Instant.now();
    jobSubmissionService.submit(userId, jobId);
    var afterSubmission = Instant.now();

    statusResponse = jobStatusService.status(userId, jobId);
    assertThat(statusResponse.status()).isEqualTo(JobStatus.SUBMITTED.toString());

    assertThat(statusResponse.jobId()).isEqualTo(jobId);
    assertThat(statusResponse.jobType()).isEqualTo(jobType);
    assertThat(statusResponse.status()).isEqualTo(JobStatus.SUBMITTED.toString());
    assertThat(statusResponse.progress()).isZero();
    assertThat(statusResponse.message()).isNull();
    assertThat(statusResponse.submittedAt()).isStrictlyBetween(beforeSubmission, afterSubmission);
    assertThat(statusResponse.createdAt()).isStrictlyBetween(beforeCreation, afterCreation);
    assertThat(statusResponse.startedAt()).isNull();
    assertThat(statusResponse.finishedAt()).isNull();

    var queueName = "queue-" + jobType;
    verify(jobQueue).publish(eq(queueName), any());
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

  private static String extractObjectKeyFromPresignedUrl(URL uploadUrl) {
    var path = uploadUrl.getPath();
    var prefix = "/" + MINIO_BUCKET + "/";
    assertThat(path).startsWith(prefix);
    return path.substring(prefix.length());
  }

  private static String newSessionId() {
    return "session-" + UUID.randomUUID();
  }
}

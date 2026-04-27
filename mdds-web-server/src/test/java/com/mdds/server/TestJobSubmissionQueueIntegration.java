/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdds.dto.JobMessageDTO;
import com.mdds.queue.Acknowledger;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import com.mdds.queue.Queue;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import io.minio.errors.MinioException;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@SpringBootTest
@Testcontainers
class TestJobSubmissionQueueIntegration {

  @Autowired private JobSubmissionService jobSubmissionService;
  @Autowired private UserLookupService userLookupService;
  @Autowired private JobCreationService jobCreationService;
  @Autowired private JobParamsService jobParamsService;
  @Autowired private JobInputUploadService jobInputUploadService;

  @Autowired
  @Qualifier("jobQueue")
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
  private static final RabbitMQContainer rabbitMq =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672)
          .waitingFor(Wait.forListeningPort())
          .withStartupTimeout(Duration.ofSeconds(30));

  @Container
  private static final MinIOContainer MINIO =
      new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z")
          .withUserName("testuser")
          .withPassword("testpassword");

  @DynamicPropertySource
  static void initProps(DynamicPropertyRegistry registry) {
    registry.add("mdds.rabbitmq.host", rabbitMq::getHost);
    registry.add("mdds.rabbitmq.port", rabbitMq::getAmqpPort);
    registry.add("mdds.rabbitmq.user", rabbitMq::getAdminUsername);
    registry.add("mdds.rabbitmq.password", rabbitMq::getAdminPassword);
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
  void testSubmissionJobIsInTheQueue(String login)
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

    var manifestObjectKey = manifestObjectKey(userId, jobId);
    var queueName = "queue-" + jobType;

    var actualMessage = new AtomicReference<JobMessageDTO>();

    var checkJobMessageHandler =
        new MessageHandler<JobMessageDTO>() {
          @Override
          public void handle(@Nonnull Message<JobMessageDTO> message, @Nonnull Acknowledger ack) {
            actualMessage.set(message.payload());
            ack.ack();
          }
        };

    try (var ignored = jobQueue.subscribe(queueName, JobMessageDTO.class, checkJobMessageHandler)) {
      jobSubmissionService.submit(userId, jobId);
      Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> actualMessage.get() != null);
    }

    assertThat(actualMessage.get()).isNotNull();
    assertThat(actualMessage.get().manifestObjectKey()).isEqualTo(manifestObjectKey);
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
        TestJobSubmissionQueueIntegration.class.getClassLoader().getResource(fileName);
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

  private static String manifestObjectKey(long userId, String jobId) {
    return "jobs/" + userId + "/" + jobId + "/manifest.json";
  }
}

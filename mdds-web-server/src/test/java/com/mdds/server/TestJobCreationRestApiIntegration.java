/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.mdds.common.util.HttpTestClient;
import com.mdds.common.util.JsonHelper;
import com.mdds.dto.CreateJobRequestDTO;
import com.mdds.dto.ErrorResponseDTO;
import com.mdds.dto.JobIdResponseDTO;
import com.mdds.server.jpa.JobsRepository;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
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
    properties = {"spring.config.import=classpath:test-job-profiles.yml"})
@Testcontainers
class TestJobCreationRestApiIntegration {
  @Autowired private JobsRepository jobsRepository;
  @LocalServerPort private int port;

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

  private static final String HOST = "localhost";

  @DynamicPropertySource
  static void initProps(DynamicPropertyRegistry registry) {
    registry.add("mdds.rabbitmq.host", RABBIT_MQ::getHost);
    registry.add("mdds.rabbitmq.port", RABBIT_MQ::getAmqpPort);
    registry.add("mdds.rabbitmq.user", RABBIT_MQ::getAdminUsername);
    registry.add("mdds.rabbitmq.password", RABBIT_MQ::getAdminPassword);
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
  }

  @Test
  void testCreateOrReuseDraftJobReturnsSameJobIdForSameUserAndUploadSession()
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var first = createOrReuseJob(http, "guest", sessionId, HttpStatus.CREATED);
    var second = createOrReuseJob(http, "guest", sessionId, HttpStatus.OK);

    assertThat(second).isEqualTo(first);
  }

  @Test
  void testCreateOrReuseDraftJobReturnsErrorWhenExistingJobNotInDraft()
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var response =
        http.post(
            "/jobs",
            Map.of(
                "Content-Type",
                "application/json",
                "X-MDDS-User-Login",
                "guest",
                "X-MDDS-Upload-Session-Id",
                sessionId),
            new CreateJobRequestDTO("solving_slae"));

    var jobId = JsonHelper.fromJson(response.body(), JobIdResponseDTO.class).getJobId();
    var jobResponse = jobsRepository.findById(jobId);
    var status = com.mdds.domain.JobStatus.SUBMITTED;
    jobResponse.ifPresent(
        job -> {
          job.setStatus(status);
          jobsRepository.save(job);
        });
    response =
        http.post(
            "/jobs",
            Map.of(
                "Content-Type",
                "application/json",
                "X-MDDS-User-Login",
                "guest",
                "X-MDDS-Upload-Session-Id",
                sessionId),
            new CreateJobRequestDTO("solving_slae"));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.CONFLICT.value());
    assertThat(message(response.body()))
        .isEqualTo(
            "Upload session id '"
                + sessionId
                + "' is already bound to job '"
                + jobId
                + "' with status '"
                + status
                + "'. A new upload session id is required.");
  }

  @Test
  void testCreateOrReuseDraftJobReturnsDifferentJobIdsForDifferentUploadSessionsOfSameUser()
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);

    var sessionA = createOrReuseJob(http, "guest", newSessionId(), HttpStatus.CREATED);
    var sessionB = createOrReuseJob(http, "guest", newSessionId(), HttpStatus.CREATED);

    assertThat(sessionB).isNotEqualTo(sessionA);
  }

  @Test
  void testCreateOrReuseDraftJobReturnsDifferentJobIdsForSameUploadSessionOfDifferentUsers()
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var guestJob = createOrReuseJob(http, "guest", sessionId, HttpStatus.CREATED);
    var adminJob = createOrReuseJob(http, "admin", sessionId, HttpStatus.CREATED);

    assertThat(adminJob).isNotEqualTo(guestJob);
  }

  private static Stream<Arguments> userValues() {
    return Stream.of(
        Arguments.of(
            "invalid_user",
            HttpURLConnection.HTTP_UNAUTHORIZED,
            "Unknown user login: invalid_user."),
        Arguments.of("", HttpURLConnection.HTTP_BAD_REQUEST, "User is null or blank."),
        Arguments.of("   ", HttpURLConnection.HTTP_BAD_REQUEST, "User is null or blank."),
        Arguments.of(" ", HttpURLConnection.HTTP_BAD_REQUEST, "User is null or blank."));
  }

  @ParameterizedTest
  @MethodSource("userValues")
  void testCreateOrReuseDraftJobReturnsErrorForInvalidUser(
      String user, int statusCode, String message) throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var response =
        http.post(
            "/jobs",
            Map.of(
                "Content-Type",
                "application/json",
                "X-MDDS-User-Login",
                user,
                "X-MDDS-Upload-Session-Id",
                newSessionId()),
            new CreateJobRequestDTO("solving_slae"));
    assertThat(response.statusCode()).isEqualTo(statusCode);
    assertThat(message(response.body())).isEqualTo(message);
  }

  private static Stream<Arguments> jobTypeValues() {
    return Stream.of(
        Arguments.of("wrong_job_type", "Unknown or unsupported job type: wrong_job_type."),
        Arguments.of(" ", "jobType: must not be null or blank."),
        Arguments.of("", "jobType: must not be null or blank."),
        Arguments.of("   ", "jobType: must not be null or blank."),
        Arguments.of(null, "jobType: must not be null or blank."));
  }

  @ParameterizedTest
  @MethodSource("jobTypeValues")
  void testCreateOrReuseDraftJobReturnsErrorForInvalidJobType(String jobType, String message)
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var response =
        http.post(
            "/jobs",
            Map.of(
                "Content-Type",
                "application/json",
                "X-MDDS-User-Login",
                "guest",
                "X-MDDS-Upload-Session-Id",
                newSessionId()),
            new CreateJobRequestDTO(jobType));
    assertThat(response.statusCode()).isEqualTo(HttpURLConnection.HTTP_BAD_REQUEST);
    assertThat(message(response.body())).isEqualTo(message);
  }

  private static Stream<String> jobSessionIdValues() {
    return Stream.of(" ", "", "   ");
  }

  @ParameterizedTest
  @MethodSource("jobSessionIdValues")
  void testCreateOrReuseDraftJobReturnsErrorForInvalidSessionId(String jobSessionId)
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var response =
        http.post(
            "/jobs",
            Map.of(
                "Content-Type",
                "application/json",
                "X-MDDS-User-Login",
                "guest",
                "X-MDDS-Upload-Session-Id",
                jobSessionId),
            new CreateJobRequestDTO("solving_slae"));
    assertThat(response.statusCode()).isEqualTo(HttpURLConnection.HTTP_BAD_REQUEST);
    assertThat(message(response.body())).isEqualTo("Upload session id is null or blank.");
  }

  private static Stream<Arguments> jsonBodyValues() {
    return Stream.of(
        Arguments.of("", "Request body is missing or malformed."),
        Arguments.of(" ", "Request body is missing or malformed."),
        Arguments.of("   ", "Request body is missing or malformed."),
        Arguments.of("{}", "jobType: must not be null or blank."),
        Arguments.of("{jobType:::malformed}", "Request body is missing or malformed."));
  }

  @ParameterizedTest
  @MethodSource("jsonBodyValues")
  void testCreateOrReuseDraftJobReturnsErrorForInvalidOrIncompleteRequestBody(
      String jsonBody, String message) throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var response =
        http.post(
            "/jobs",
            Map.of(
                "Content-Type",
                "application/json",
                "X-MDDS-User-Login",
                "guest",
                "X-MDDS-Upload-Session-Id",
                newSessionId()),
            jsonBody);
    assertThat(response.statusCode()).isEqualTo(HttpURLConnection.HTTP_BAD_REQUEST);
    assertThat(message(response.body())).isEqualTo(message);
  }

  @Test
  void testCreateOrReuseDraftJobReturnsErrorForMissingUser()
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var response =
        http.post(
            "/jobs",
            Map.of("Content-Type", "application/json", "X-MDDS-Upload-Session-Id", newSessionId()),
            new CreateJobRequestDTO("solving_slae"));
    assertThat(response.statusCode()).isEqualTo(HttpURLConnection.HTTP_BAD_REQUEST);
    assertThat(message(response.body()))
        .isEqualTo("Required request header 'X-MDDS-User-Login' is missing.");
  }

  @Test
  void testCreateOrReuseDraftJobReturnsErrorForMissingSession()
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var response =
        http.post(
            "/jobs",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", "guest"),
            new CreateJobRequestDTO("solving_slae"));
    assertThat(response.statusCode()).isEqualTo(HttpURLConnection.HTTP_BAD_REQUEST);
    assertThat(message(response.body()))
        .isEqualTo("Required request header 'X-MDDS-Upload-Session-Id' is missing.");
  }

  @Test
  void testCreateOrReuseDraftJobReturnsErrorForConflictingJobTypes()
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var originJobType = "solving_slae";
    var otherJobType = "solving_slae_parallel";
    var response =
        http.post(
            "/jobs",
            Map.of(
                "Content-Type",
                "application/json",
                "X-MDDS-User-Login",
                "guest",
                "X-MDDS-Upload-Session-Id",
                sessionId),
            new CreateJobRequestDTO(originJobType));
    assertThat(response.statusCode()).isEqualTo(HttpStatus.CREATED.value());
    response =
        http.post(
            "/jobs",
            Map.of(
                "Content-Type",
                "application/json",
                "X-MDDS-User-Login",
                "guest",
                "X-MDDS-Upload-Session-Id",
                sessionId),
            new CreateJobRequestDTO(otherJobType));
    assertThat(response.statusCode()).isEqualTo(HttpStatus.CONFLICT.value());
    assertThat(message(response.body()))
        .isEqualTo(
            "A draft job already exists for upload session id '"
                + sessionId
                + "' with job type '"
                + originJobType
                + "', which does not match requested job type '"
                + otherJobType
                + "'.");
  }

  @Test
  void testCreateOrReuseDraftJobReturnsErrorForNonJsonContentType()
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var response =
        http.post(
            "/jobs",
            Map.of(
                "Content-Type",
                "application/xml",
                "X-MDDS-User-Login",
                "guest",
                "X-MDDS-Upload-Session-Id",
                sessionId),
            new CreateJobRequestDTO("solving_slae"));
    assertThat(response.statusCode()).isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE.value());
  }

  @Test
  void testCreateOrReuseDraftJobReturnsErrorForMissingContentType()
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var response =
        http.post(
            "/jobs",
            Map.of("X-MDDS-User-Login", "guest", "X-MDDS-Upload-Session-Id", sessionId),
            new CreateJobRequestDTO("solving_slae"));
    assertThat(response.statusCode()).isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE.value());
  }

  private JobIdResponseDTO createOrReuseJob(
      HttpTestClient http, String userLogin, String uploadSessionId, HttpStatus status)
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
            new CreateJobRequestDTO("solving_slae"));

    assertThat(response.statusCode()).isEqualTo(status.value());

    var dto = JsonHelper.fromJson(response.body(), JobIdResponseDTO.class);
    assertThat(dto).isNotNull();
    assertThat(dto.getJobId()).isNotBlank();
    return dto;
  }

  private static String message(String rawJson) {
    return JsonHelper.fromJson(rawJson, ErrorResponseDTO.class).message();
  }

  private static String newSessionId() {
    return "session-" + UUID.randomUUID();
  }
}

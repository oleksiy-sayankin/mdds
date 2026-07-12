/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.mdds.common.util.HttpTestClient;
import com.mdds.common.util.JsonHelper;
import com.mdds.domain.JobStatus;
import com.mdds.dto.rest.v1.CancelJobResponseDTO;
import com.mdds.dto.rest.v1.CreateJobRequestDTO;
import com.mdds.dto.rest.v1.CreateJobResponseDTO;
import com.mdds.dto.rest.v1.ErrorResponseDTO;
import com.mdds.server.support.JobTestFixture;
import java.io.IOException;
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
import org.springframework.context.annotation.Import;
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
    properties = "mdds.job-profile.mode=yaml")
@Testcontainers
@Import(JobTestFixture.class)
class TestJobCancellationRestApiIntegration {

  @LocalServerPort private int port;

  @Autowired private JobTestFixture jobFixture;

  private static final String HOST = "localhost";
  private static final String GUEST = "guest";
  private static final String ADMIN = "admin";

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

  private static Stream<String> userLoginValues() {
    return Stream.of(GUEST, ADMIN);
  }

  @ParameterizedTest
  @MethodSource("userLoginValues")
  void testCancellation(String login) throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, login, sessionId, jobType);
    var jobId = createJobResponse.jobId();

    var workerId = newWorkerId();
    jobFixture.forceStatus(jobId, JobStatus.IN_PROGRESS);
    jobFixture.forceWorkerId(jobId, workerId);

    var result = cancel(http, login, jobId);
    assertThat(result.jobId()).isEqualTo(jobId);
    assertThat(result.status()).isEqualTo("CANCEL_REQUESTED");
  }

  private static Stream<JobStatus> jobTerminalStatusValues() {
    return Stream.of(
        JobStatus.CANCELLED, JobStatus.DONE, JobStatus.ERROR, JobStatus.VALIDATION_FAILED);
  }

  @ParameterizedTest
  @MethodSource("jobTerminalStatusValues")
  void testCancellationInTerminalJobState(JobStatus jobStatus)
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.jobId();

    var workerId = newWorkerId();
    jobFixture.forceStatus(jobId, jobStatus);
    jobFixture.forceWorkerId(jobId, workerId);

    var response =
        http.post(
            "/jobs/" + jobId + "/cancel",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.CONFLICT.value());
    assertThat(message(response.body()))
        .isEqualTo(
            "Job '"
                + jobId
                + "' is in terminal state '"
                + jobStatus.getCode()
                + "' and cancellation is not allowed.");
  }

  private static Stream<JobStatus> jobInvalidStatusValues() {
    return Stream.of(JobStatus.DRAFT, JobStatus.SUBMITTED);
  }

  @ParameterizedTest
  @MethodSource("jobInvalidStatusValues")
  void testCancellationInvalidJobState(JobStatus jobStatus)
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.jobId();

    var workerId = newWorkerId();
    jobFixture.forceStatus(jobId, jobStatus);
    jobFixture.forceWorkerId(jobId, workerId);

    var response =
        http.post(
            "/jobs/" + jobId + "/cancel",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.CONFLICT.value());
    assertThat(message(response.body()))
        .isEqualTo(
            "Job '"
                + jobId
                + "' is in state '"
                + jobStatus.getCode()
                + "' and cancellation is supported only for 'IN_PROGRESS' jobs.");
  }

  @Test
  void testCancellationJobFromOtherUser() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, ADMIN, sessionId, jobType);
    var jobId = createJobResponse.jobId();

    var workerId = newWorkerId();
    jobFixture.forceStatus(jobId, JobStatus.IN_PROGRESS);
    jobFixture.forceWorkerId(jobId, workerId);

    var response =
        http.post(
            "/jobs/" + jobId + "/cancel",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.NOT_FOUND.value());
    assertThat(message(response.body())).isEqualTo("Job with id '" + jobId + "' does not exist.");
  }

  @Test
  void testCancellationNoWorker() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.jobId();
    jobFixture.forceStatus(jobId, JobStatus.IN_PROGRESS);

    var response =
        http.post(
            "/jobs/" + jobId + "/cancel",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
    assertThat(message(response.body())).isEqualTo("Internal Server Error");
  }

  @Test
  void testCancellationNoJob() throws IOException, InterruptedException {
    var jobId = "invalid-job-id";
    var http = new HttpTestClient(HOST, port);
    var response =
        http.post(
            "/jobs/" + jobId + "/cancel",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", GUEST));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.NOT_FOUND.value());
    assertThat(message(response.body())).isEqualTo("Job with id '" + jobId + "' does not exist.");
  }

  @Test
  void testCancellationWhenCancelAlreadyRequested() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.jobId();

    var workerId = newWorkerId();
    jobFixture.forceStatus(jobId, JobStatus.IN_PROGRESS);
    jobFixture.forceWorkerId(jobId, workerId);

    var result = cancel(http, GUEST, jobId);
    assertThat(result.jobId()).isEqualTo(jobId);
    assertThat(result.status()).isEqualTo("CANCEL_REQUESTED");

    result = cancel(http, GUEST, jobId);
    assertThat(result.jobId()).isEqualTo(jobId);
    assertThat(result.status()).isEqualTo("CANCEL_REQUESTED");
  }

  @Test
  void testErrorForMissingUser() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.jobId();

    var workerId = newWorkerId();
    jobFixture.forceStatus(jobId, JobStatus.IN_PROGRESS);
    jobFixture.forceWorkerId(jobId, workerId);

    var response =
        http.post("/jobs/" + jobId + "/cancel", Map.of("Content-Type", "application/json"));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(message(response.body()))
        .isEqualTo("Required request header 'X-MDDS-User-Login' is missing.");
  }

  @Test
  void testOkForMissingContentType() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.jobId();

    var workerId = newWorkerId();
    jobFixture.forceStatus(jobId, JobStatus.IN_PROGRESS);
    jobFixture.forceWorkerId(jobId, workerId);

    var response = http.post("/jobs/" + jobId + "/cancel", Map.of("X-MDDS-User-Login", GUEST));
    assertThat(response.statusCode()).isEqualTo(HttpStatus.ACCEPTED.value());
    var result = JsonHelper.fromJson(response.body(), CancelJobResponseDTO.class);
    assertThat(result.jobId()).isEqualTo(jobId);
    assertThat(result.status()).isEqualTo("CANCEL_REQUESTED");
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
    var jobId = createJobResponse.jobId();

    var workerId = newWorkerId();
    jobFixture.forceStatus(jobId, JobStatus.IN_PROGRESS);
    jobFixture.forceWorkerId(jobId, workerId);

    var response =
        http.post(
            "/jobs/" + jobId + "/cancel",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", user));

    assertThat(response.statusCode()).isEqualTo(statusCode);
    assertThat(message(response.body())).isEqualTo(message);
  }

  private static String newSessionId() {
    return "session-" + UUID.randomUUID();
  }

  private static CreateJobResponseDTO createOrReuseJob(
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

    var dto = JsonHelper.fromJson(response.body(), CreateJobResponseDTO.class);
    assertThat(dto).isNotNull();
    assertThat(dto.jobId()).isNotBlank();
    return dto;
  }

  private static CancelJobResponseDTO cancel(HttpTestClient http, String userLogin, String jobId)
      throws IOException, InterruptedException {
    var response =
        http.post(
            "/jobs/" + jobId + "/cancel",
            Map.of("Content-Type", "application/json", "X-MDDS-User-Login", userLogin));

    assertThat(response.statusCode()).isEqualTo(HttpStatus.ACCEPTED.value());

    return JsonHelper.fromJson(response.body(), CancelJobResponseDTO.class);
  }

  private static String newWorkerId() {
    return "worker-" + UUID.randomUUID();
  }

  private static String message(String rawJson) {
    return JsonHelper.fromJson(rawJson, ErrorResponseDTO.class).message();
  }
}

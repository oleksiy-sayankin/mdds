/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdds.common.util.HttpTestClient;
import com.mdds.common.util.JsonHelper;
import com.mdds.domain.JobStatus;
import com.mdds.dto.CreateJobRequestDTO;
import com.mdds.dto.ErrorResponseDTO;
import com.mdds.dto.JobIdResponseDTO;
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
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@Import(JobTestFixture.class)
class TestJobParamsRestApiIntegration {
  @LocalServerPort private int port;
  @Autowired private JobTestFixture jobFixture;

  private static final String HOST = "localhost";
  private static final String GUEST = "guest";
  private static final String ADMIN = "admin";
  private static final ObjectMapper MAPPER = new ObjectMapper();

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

  @DynamicPropertySource
  static void initProps(DynamicPropertyRegistry registry) {
    registry.add("mdds.rabbitmq.host", rabbitMq::getHost);
    registry.add("mdds.rabbitmq.port", rabbitMq::getAmqpPort);
    registry.add("mdds.rabbitmq.user", rabbitMq::getAdminUsername);
    registry.add("mdds.rabbitmq.password", rabbitMq::getAdminPassword);
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
  }

  private static Stream<String> userLoginValues() {
    return Stream.of(GUEST, ADMIN);
  }

  @ParameterizedTest
  @MethodSource("userLoginValues")
  void testAddJobParamsViaRestApi(String userLogin) throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, userLogin, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var paramsAsMap = Map.of(paramName, paramValue);
    var paramsAsJson = JsonHelper.toJson(paramsAsMap);
    patchParams(http, userLogin, jobId, paramsAsJson);
    var params = jobFixture.jobParams(jobId);
    assertThat(params)
        .containsKey("solvingMethod")
        .containsValue(MAPPER.readTree("\"numpy_exact_solver\""));
  }

  @Test
  void testUpdateExistingJobParams() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var solvingMethod = "solvingMethod";
    var solvingMethodValue = MAPPER.readTree("\"numpy_exact_solver\"");

    var precision = "tolerance";
    var precisionValue = MAPPER.readTree("0.001");

    var paramsAsJson =
        JsonHelper.toJson(Map.of(solvingMethod, solvingMethodValue, precision, precisionValue));
    patchParams(http, GUEST, jobId, paramsAsJson);
    var params = jobFixture.jobParams(jobId);

    assertThat(params)
        .containsKey(solvingMethod)
        .containsValue(solvingMethodValue)
        .containsKey(precision)
        .containsValue(precisionValue);

    var newSolvingMethodValue = MAPPER.readTree("\"numpy_pinv_solver\"");
    var newPrecisionValue = MAPPER.readTree("0.00001");

    var newParamsAsJson =
        JsonHelper.toJson(
            Map.of(solvingMethod, newSolvingMethodValue, precision, newPrecisionValue));
    patchParams(http, GUEST, jobId, newParamsAsJson);

    params = jobFixture.jobParams(jobId);
    assertThat(params)
        .containsKey(solvingMethod)
        .containsValue(newSolvingMethodValue)
        .containsKey(precision)
        .containsValue(newPrecisionValue);
  }

  @Test
  void testKeepAsIsOmittedJobParam() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var solvingMethod = "solvingMethod";
    var solvingMethodValue = MAPPER.readTree("\"numpy_exact_solver\"");

    var precision = "tolerance";
    var precisionValue = MAPPER.readTree("0.001");

    var paramsAsJson =
        JsonHelper.toJson(Map.of(solvingMethod, solvingMethodValue, precision, precisionValue));
    patchParams(http, GUEST, jobId, paramsAsJson);
    var params = jobFixture.jobParams(jobId);

    assertThat(params)
        .containsKey(solvingMethod)
        .containsValue(solvingMethodValue)
        .containsKey(precision)
        .containsValue(precisionValue);

    var newSolvingMethodValue = MAPPER.readTree("\"numpy_pinv_solver\"");

    var newParamsAsJson = JsonHelper.toJson(Map.of(solvingMethod, newSolvingMethodValue));
    patchParams(http, GUEST, jobId, newParamsAsJson);

    params = jobFixture.jobParams(jobId);
    assertThat(params)
        .containsKey(solvingMethod)
        .containsValue(newSolvingMethodValue)
        .containsKey(precision)
        .containsValue(precisionValue);
  }

  @Test
  void testDeleteOneKeepOthers() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var solvingMethod = "solvingMethod";
    var solvingMethodValue = MAPPER.readTree("\"numpy_exact_solver\"");

    var precision = "tolerance";
    var precisionValue = MAPPER.readTree("0.001");

    var paramsAsJson =
        JsonHelper.toJson(Map.of(solvingMethod, solvingMethodValue, precision, precisionValue));
    patchParams(http, GUEST, jobId, paramsAsJson);
    var params = jobFixture.jobParams(jobId);

    assertThat(params)
        .containsKey(solvingMethod)
        .containsValue(solvingMethodValue)
        .containsKey(precision)
        .containsValue(precisionValue);

    var newSolvingMethodValue = MAPPER.readTree("null");

    var newParamsAsJson = JsonHelper.toJson(Map.of(solvingMethod, newSolvingMethodValue));
    patchParams(http, GUEST, jobId, newParamsAsJson);

    params = jobFixture.jobParams(jobId);
    assertThat(params)
        .doesNotContainKey(solvingMethod)
        .containsKey(precision)
        .containsValue(precisionValue);
  }

  @Test
  void testNoOperation() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var solvingMethod = "solvingMethod";
    var solvingMethodValue = MAPPER.readTree("\"numpy_exact_solver\"");

    var precision = "tolerance";
    var precisionValue = MAPPER.readTree("0.001");

    var paramsAsJson =
        JsonHelper.toJson(Map.of(solvingMethod, solvingMethodValue, precision, precisionValue));
    patchParams(http, GUEST, jobId, paramsAsJson);
    var params = jobFixture.jobParams(jobId);

    assertThat(params)
        .containsKey(solvingMethod)
        .containsValue(solvingMethodValue)
        .containsKey(precision)
        .containsValue(precisionValue);

    var newParamsAsJson = "{}";
    patchParams(http, GUEST, jobId, newParamsAsJson);

    params = jobFixture.jobParams(jobId);
    assertThat(params)
        .containsKey(solvingMethod)
        .containsValue(solvingMethodValue)
        .containsKey(precision)
        .containsValue(precisionValue);
  }

  @Test
  void testClearExistingJobParams() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var solvingMethod = "solvingMethod";
    var solvingMethodValue = MAPPER.readTree("\"numpy_exact_solver\"");

    var precision = "tolerance";
    var precisionValue = MAPPER.readTree("0.001");

    var paramsAsJson =
        JsonHelper.toJson(Map.of(solvingMethod, solvingMethodValue, precision, precisionValue));
    patchParams(http, GUEST, jobId, paramsAsJson);
    var params = jobFixture.jobParams(jobId);

    assertThat(params)
        .containsKey(solvingMethod)
        .containsValue(solvingMethodValue)
        .containsKey(precision)
        .containsValue(precisionValue);

    var nullSolvingMethodValue = MAPPER.readTree("null");
    var nullPrecisionValue = MAPPER.readTree("null");
    var newParamsAsJson =
        JsonHelper.toJson(
            Map.of(solvingMethod, nullSolvingMethodValue, precision, nullPrecisionValue));
    patchParams(http, GUEST, jobId, newParamsAsJson);

    params = jobFixture.jobParams(jobId);
    assertThat(params).isEmpty();
  }

  @Test
  void testJobDoesNotExist() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var jobId = "wrong_job_id";
    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var paramsAsMap = Map.of(paramName, paramValue);
    var paramsAsJson = JsonHelper.toJson(paramsAsMap);

    var response =
        http.patch(
            "/jobs/" + jobId + "/params",
            Map.of("Content-Type", "application/merge-patch+json", "X-MDDS-User-Login", GUEST),
            paramsAsJson);

    assertThat(response.statusCode()).isEqualTo(HttpStatus.NOT_FOUND.value());
    assertThat(message(response.body())).isEqualTo("Job with id '" + jobId + "' does not exist.");
  }

  @ParameterizedTest
  @MethodSource("mapParametersValues")
  void testJobDoesNotExistForOtherUser(Map<String, JsonNode> params)
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, ADMIN, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var paramsAsJson = JsonHelper.toJson(params);

    var response =
        http.patch(
            "/jobs/" + jobId + "/params",
            Map.of("Content-Type", "application/merge-patch+json", "X-MDDS-User-Login", GUEST),
            paramsAsJson);

    assertThat(response.statusCode()).isEqualTo(HttpStatus.NOT_FOUND.value());
    assertThat(message(response.body())).isEqualTo("Job with id '" + jobId + "' does not exist.");
  }

  private static Stream<String> invalidJsonDoc() {
    return Stream.of("[]", "123", "null", "\"solvingMethod\"", "\"numpy_exact_solver\"");
  }

  @ParameterizedTest
  @MethodSource("invalidJsonDoc")
  void testInvalidPatchJsonDocument(String paramsAsJson) throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();

    var response =
        http.patch(
            "/jobs/" + jobId + "/params",
            Map.of("Content-Type", "application/merge-patch+json", "X-MDDS-User-Login", GUEST),
            paramsAsJson);

    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(message(response.body()))
        .isEqualTo("The merge patch document must be a JSON object.");
  }

  private static Stream<Map<String, JsonNode>> mapParametersValues()
      throws JsonProcessingException {
    return Stream.of(Map.of("solvingMethod", MAPPER.readTree("\"numpy_exact_solver\"")), Map.of());
  }

  @ParameterizedTest
  @MethodSource("mapParametersValues")
  void testJobIsNotDraft(Map<String, JsonNode> params) throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var paramsAsJson = JsonHelper.toJson(params);
    jobFixture.forceStatus(jobId, JobStatus.SUBMITTED);

    var response =
        http.patch(
            "/jobs/" + jobId + "/params",
            Map.of("Content-Type", "application/merge-patch+json", "X-MDDS-User-Login", GUEST),
            paramsAsJson);

    assertThat(response.statusCode()).isEqualTo(HttpStatus.CONFLICT.value());
    assertThat(message(response.body()))
        .isEqualTo(
            "Job '" + jobId + "' is not in DRAFT state and no more job parameters can be patched.");
  }

  @Test
  void testJobParameterIsNullOrBlank() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var paramName = "";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var paramsAsMap = Map.of(paramName, paramValue);
    var paramsAsJson = JsonHelper.toJson(paramsAsMap);

    var response =
        http.patch(
            "/jobs/" + jobId + "/params",
            Map.of("Content-Type", "application/merge-patch+json", "X-MDDS-User-Login", GUEST),
            paramsAsJson);

    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(message(response.body())).isEqualTo("Parameter name is blank or invalid.");
  }

  private static Stream<String> parametersValues() {
    return Stream.of("unknown_parameter", "SolvingMethod", "solvingmethod", "SOLVINGMETHOD");
  }

  @ParameterizedTest
  @MethodSource("parametersValues")
  void testUnknownOrUnsupportedJobParameter(String paramName)
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var paramsAsMap = Map.of(paramName, paramValue);
    var paramsAsJson = JsonHelper.toJson(paramsAsMap);

    var response =
        http.patch(
            "/jobs/" + jobId + "/params",
            Map.of("Content-Type", "application/merge-patch+json", "X-MDDS-User-Login", GUEST),
            paramsAsJson);

    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(message(response.body()))
        .isEqualTo(
            "Unknown or unsupported parameter '"
                + paramName
                + "' for the given job type: '"
                + jobType
                + "'.");
  }

  private static Stream<Arguments> inputParams() throws JsonProcessingException {
    return Stream.of(
        Arguments.of(
            "solvingMethod",
            MAPPER.readTree("1.29"),
            "Parameter value '1.29' for parameter 'solvingMethod' has an invalid type 'number' for"
                + " the given job type 'solving_slae'."),
        Arguments.of(
            "tolerance",
            MAPPER.readTree("\"abc\""),
            "Parameter value 'abc' for parameter 'tolerance' has an invalid type 'string' for the"
                + " given job type 'solving_slae'."),
        Arguments.of(
            "solvingMethod",
            MAPPER.readTree("\"wrong_param_value\""),
            "Invalid value 'wrong_param_value' of parameter 'solvingMethod' for the given job type"
                + " 'solving_slae'."));
  }

  @ParameterizedTest
  @MethodSource("inputParams")
  void testInvalidJobParameterInput(String paramName, JsonNode paramValue, String message)
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var paramsAsMap = Map.of(paramName, paramValue);
    var paramsAsJson = JsonHelper.toJson(paramsAsMap);

    var response =
        http.patch(
            "/jobs/" + jobId + "/params",
            Map.of("Content-Type", "application/merge-patch+json", "X-MDDS-User-Login", GUEST),
            paramsAsJson);

    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(message(response.body())).isEqualTo(message);
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
    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var paramsAsMap = Map.of(paramName, paramValue);
    var paramsAsJson = JsonHelper.toJson(paramsAsMap);

    var response =
        http.patch(
            "/jobs/" + jobId + "/params",
            Map.of("Content-Type", "application/merge-patch+json", "X-MDDS-User-Login", user),
            paramsAsJson);

    assertThat(response.statusCode()).isEqualTo(statusCode);
    assertThat(message(response.body())).isEqualTo(message);
  }

  @Test
  void testErrorForInvalidContentType() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var paramsAsMap = Map.of(paramName, paramValue);
    var paramsAsJson = JsonHelper.toJson(paramsAsMap);

    var response =
        http.patch(
            "/jobs/" + jobId + "/params",
            Map.of("Content-Type", "application/xml", "X-MDDS-User-Login", GUEST),
            paramsAsJson);
    assertThat(response.statusCode()).isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE.value());
  }

  @Test
  void testErrorForMissingContentType() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var paramsAsMap = Map.of(paramName, paramValue);
    var paramsAsJson = JsonHelper.toJson(paramsAsMap);

    var response =
        http.patch("/jobs/" + jobId + "/params", Map.of("X-MDDS-User-Login", GUEST), paramsAsJson);
    assertThat(response.statusCode()).isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE.value());
  }

  @Test
  void testErrorForMissingUser() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var sessionId = newSessionId();
    var jobType = "solving_slae";
    var createJobResponse = createOrReuseJob(http, GUEST, sessionId, jobType);
    var jobId = createJobResponse.getJobId();
    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var paramsAsMap = Map.of(paramName, paramValue);
    var paramsAsJson = JsonHelper.toJson(paramsAsMap);

    var response =
        http.patch(
            "/jobs/" + jobId + "/params",
            Map.of("Content-Type", "application/merge-patch+json"),
            paramsAsJson);
    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(message(response.body()))
        .isEqualTo("Required request header 'X-MDDS-User-Login' is missing.");
  }

  private static Stream<Arguments> jsonBodyValues() {
    return Stream.of(
        Arguments.of("", "Request body is missing or malformed."),
        Arguments.of(" ", "Request body is missing or malformed."),
        Arguments.of("   ", "Request body is missing or malformed."),
        Arguments.of("{some_job_param:::malformed}", "Request body is missing or malformed."));
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
        http.patch(
            "/jobs/" + jobId + "/params",
            Map.of("Content-Type", "application/merge-patch+json", "X-MDDS-User-Login", GUEST),
            jsonBody);
    assertThat(response.statusCode()).isEqualTo(HttpStatus.BAD_REQUEST.value());
    assertThat(message(response.body())).isEqualTo(message);
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

  private static String message(String rawJson) {
    return JsonHelper.fromJson(rawJson, ErrorResponseDTO.class).message();
  }
}

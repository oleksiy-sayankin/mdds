/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.server.JsonTypeFormatter.describeJsonType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdds.domain.JobStatus;
import com.mdds.server.support.JobTestFixture;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
@Import(JobTestFixture.class)
class TestJobParamsServiceIntegration {

  @Autowired private JobCreationService jobCreationService;
  @Autowired private UserLookupService userLookupService;
  @Autowired private JobParamsService jobParamsService;
  @Autowired private JobTestFixture jobFixture;

  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17")
          .withDatabaseName("mdds")
          .withUsername("mdds")
          .withPassword("mdds123");

  @DynamicPropertySource
  static void initProps(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
  }

  private static final String GUEST = "guest";
  private static final String ADMIN = "admin";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static Stream<String> userLoginValues() {
    return Stream.of(GUEST, ADMIN);
  }

  @ParameterizedTest
  @MethodSource("userLoginValues")
  void testStoreSingleJobParameter(String login) throws JsonProcessingException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(login);
    var result = createOrReuseDraftJob(userId, session, jobType);
    var jobId = result.jobId();

    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var params = Map.of(paramName, paramValue);
    jobParamsService.mergeParams(userId, jobId, params);
    var actualParams = jobFixture.jobParams(jobId);
    assertThat(actualParams).containsKey("solvingMethod").containsValue(paramValue);
  }

  @Test
  void testUpdateExistingJobParams() throws JsonProcessingException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var result = createOrReuseDraftJob(userId, session, jobType);
    var jobId = result.jobId();

    var solvingMethod = "solvingMethod";
    var solvingMethodValue = MAPPER.readTree("\"numpy_exact_solver\"");

    var precision = "tolerance";
    var precisionValue = MAPPER.readTree("0.001");

    jobParamsService.mergeParams(
        userId, jobId, Map.of(solvingMethod, solvingMethodValue, precision, precisionValue));
    var params = jobFixture.jobParams(jobId);
    assertThat(params)
        .containsKey(solvingMethod)
        .containsValue(solvingMethodValue)
        .containsKey(precision)
        .containsValue(precisionValue);

    var newSolvingMethodValue = MAPPER.readTree("\"numpy_pinv_solver\"");
    var newPrecisionValue = MAPPER.readTree("0.00001");

    jobParamsService.mergeParams(
        userId, jobId, Map.of(solvingMethod, newSolvingMethodValue, precision, newPrecisionValue));
    params = jobFixture.jobParams(jobId);
    assertThat(params)
        .containsKey(solvingMethod)
        .containsValue(newSolvingMethodValue)
        .containsKey(precision)
        .containsValue(newPrecisionValue);
  }

  @Test
  void testKeepAsIsOmittedJobParam() throws JsonProcessingException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var result = createOrReuseDraftJob(userId, session, jobType);
    var jobId = result.jobId();

    var solvingMethod = "solvingMethod";
    var solvingMethodValue = MAPPER.readTree("\"numpy_exact_solver\"");

    var precision = "tolerance";
    var precisionValue = MAPPER.readTree("0.001");

    jobParamsService.mergeParams(
        userId, jobId, Map.of(solvingMethod, solvingMethodValue, precision, precisionValue));
    var params = jobFixture.jobParams(jobId);
    assertThat(params)
        .containsKey(solvingMethod)
        .containsValue(solvingMethodValue)
        .containsKey(precision)
        .containsValue(precisionValue);

    var newSolvingMethodValue = MAPPER.readTree("\"numpy_pinv_solver\"");

    jobParamsService.mergeParams(userId, jobId, Map.of(solvingMethod, newSolvingMethodValue));
    params = jobFixture.jobParams(jobId);
    assertThat(params)
        .containsKey(solvingMethod)
        .containsValue(newSolvingMethodValue)
        .containsKey(precision)
        .containsValue(precisionValue);
  }

  @Test
  void testDeleteOneKeepOthers() throws JsonProcessingException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var result = createOrReuseDraftJob(userId, session, jobType);
    var jobId = result.jobId();

    var solvingMethod = "solvingMethod";
    var solvingMethodValue = MAPPER.readTree("\"numpy_exact_solver\"");

    var precision = "tolerance";
    var precisionValue = MAPPER.readTree("0.001");

    jobParamsService.mergeParams(
        userId, jobId, Map.of(solvingMethod, solvingMethodValue, precision, precisionValue));
    var params = jobFixture.jobParams(jobId);
    assertThat(params)
        .containsKey(solvingMethod)
        .containsValue(solvingMethodValue)
        .containsKey(precision)
        .containsValue(precisionValue);

    var newSolvingMethodValue = MAPPER.readTree("null");

    jobParamsService.mergeParams(userId, jobId, Map.of(solvingMethod, newSolvingMethodValue));
    params = jobFixture.jobParams(jobId);
    assertThat(params)
        .doesNotContainKey(solvingMethod)
        .containsKey(precision)
        .containsValue(precisionValue);
  }

  @Test
  void testNoOperation() throws JsonProcessingException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var result = createOrReuseDraftJob(userId, session, jobType);
    var jobId = result.jobId();

    var solvingMethod = "solvingMethod";
    var solvingMethodValue = MAPPER.readTree("\"numpy_exact_solver\"");

    var precision = "tolerance";
    var precisionValue = MAPPER.readTree("0.001");

    jobParamsService.mergeParams(
        userId, jobId, Map.of(solvingMethod, solvingMethodValue, precision, precisionValue));
    var params = jobFixture.jobParams(jobId);
    assertThat(params)
        .containsKey(solvingMethod)
        .containsValue(solvingMethodValue)
        .containsKey(precision)
        .containsValue(precisionValue);

    jobParamsService.mergeParams(userId, jobId, Map.of());
    params = jobFixture.jobParams(jobId);
    assertThat(params)
        .containsKey(solvingMethod)
        .containsValue(solvingMethodValue)
        .containsKey(precision)
        .containsValue(precisionValue);
  }

  @Test
  void testClearExistingJobParams() throws JsonProcessingException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var result = createOrReuseDraftJob(userId, session, jobType);
    var jobId = result.jobId();

    var solvingMethod = "solvingMethod";
    var solvingMethodValue = MAPPER.readTree("\"numpy_exact_solver\"");

    var precision = "tolerance";
    var precisionValue = MAPPER.readTree("0.001");

    jobParamsService.mergeParams(
        userId, jobId, Map.of(solvingMethod, solvingMethodValue, precision, precisionValue));
    var params = jobFixture.jobParams(jobId);
    assertThat(params)
        .containsKey(solvingMethod)
        .containsValue(solvingMethodValue)
        .containsKey(precision)
        .containsValue(precisionValue);

    var nullSolvingMethodValue = MAPPER.readTree("null");
    var nullPrecisionValue = MAPPER.readTree("null");

    jobParamsService.mergeParams(
        userId,
        jobId,
        Map.of(solvingMethod, nullSolvingMethodValue, precision, nullPrecisionValue));
    params = jobFixture.jobParams(jobId);
    assertThat(params).isEmpty();
  }

  @Test
  void testJobDoesNotExist() throws JsonProcessingException {
    var userId = userLookupService.findUserId(GUEST);
    var jobId = "wrong_job_id";
    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var params = Map.of(paramName, paramValue);
    assertThatExceptionOfType(JobDoesNotExistException.class)
        .isThrownBy(() -> jobParamsService.mergeParams(userId, jobId, params))
        .withMessage("Job with id '" + jobId + "' does not exist.");
  }

  @ParameterizedTest
  @MethodSource("mapParametersValues")
  void testJobDoesNotExistForOtherUser(Map<String, JsonNode> params) {
    var session = newSessionId();
    var jobType = "solving_slae";
    var adminId = userLookupService.findUserId(ADMIN);
    var result = createOrReuseDraftJob(adminId, session, jobType);
    var jobId = result.jobId();
    var guestId = userLookupService.findUserId(GUEST);
    assertThatExceptionOfType(JobDoesNotExistException.class)
        .isThrownBy(() -> jobParamsService.mergeParams(guestId, jobId, params))
        .withMessage("Job with id '" + jobId + "' does not exist.");
  }

  private static Stream<Map<String, JsonNode>> mapParametersValues()
      throws JsonProcessingException {
    return Stream.of(Map.of("solvingMethod", MAPPER.readTree("\"numpy_exact_solver\"")), Map.of());
  }

  @ParameterizedTest
  @MethodSource("mapParametersValues")
  void testJobIsNotDraft(Map<String, JsonNode> params) {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var result = createOrReuseDraftJob(userId, session, jobType);
    var jobId = result.jobId();
    jobFixture.forceStatus(jobId, JobStatus.SUBMITTED);

    assertThatExceptionOfType(JobIsNotDraftException.class)
        .isThrownBy(() -> jobParamsService.mergeParams(userId, jobId, params))
        .withMessage(
            "Job '" + jobId + "' is not in DRAFT state and no more job parameters can be patched.");
  }

  @Test
  void testJobParameterIsNullOrBlank() throws JsonProcessingException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var result = createOrReuseDraftJob(userId, session, jobType);
    var jobId = result.jobId();

    var paramName = "";
    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var params = Map.of(paramName, paramValue);
    assertThatExceptionOfType(JobParameterIsNullOrBlankException.class)
        .isThrownBy(() -> jobParamsService.mergeParams(userId, jobId, params))
        .withMessage("Parameter name is blank or invalid.");
  }

  private static Stream<String> parametersValues() {
    return Stream.of("unknown_parameter", "SolvingMethod", "solvingmethod", "SOLVINGMETHOD");
  }

  @ParameterizedTest
  @MethodSource("parametersValues")
  void testUnknownOrUnsupportedJobParameter(String paramName) throws JsonProcessingException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var result = createOrReuseDraftJob(userId, session, jobType);
    var jobId = result.jobId();

    var paramValue = MAPPER.readTree("\"numpy_exact_solver\"");
    var params = Map.of(paramName, paramValue);
    assertThatExceptionOfType(UnknownOrUnsupportedJobParameterException.class)
        .isThrownBy(() -> jobParamsService.mergeParams(userId, jobId, params))
        .withMessage(
            "Unknown or unsupported parameter '"
                + paramName
                + "' for the given job type: '"
                + jobType
                + "'.");
  }

  @Test
  void testInvalidStringJobParameterType() throws JsonProcessingException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var result = createOrReuseDraftJob(userId, session, jobType);
    var jobId = result.jobId();

    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("1.29");
    var paramType = paramValue.getNodeType();
    var params = Map.of(paramName, paramValue);
    assertThatExceptionOfType(InvalidJobParameterTypeException.class)
        .isThrownBy(() -> jobParamsService.mergeParams(userId, jobId, params))
        .withMessage(
            "Parameter value '"
                + paramValue.asText()
                + "' for parameter '"
                + paramName
                + "' has an invalid type '"
                + describeJsonType(paramType)
                + "' for the given job type"
                + " '"
                + jobType
                + "'.");
  }

  @Test
  void testInvalidNumericJobParameterType() throws JsonProcessingException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var result = createOrReuseDraftJob(userId, session, jobType);
    var jobId = result.jobId();

    var paramName = "tolerance";
    var paramValue = MAPPER.readTree("\"abc\"");
    var paramType = paramValue.getNodeType();
    var params = Map.of(paramName, paramValue);
    assertThatExceptionOfType(InvalidJobParameterTypeException.class)
        .isThrownBy(() -> jobParamsService.mergeParams(userId, jobId, params))
        .withMessage(
            "Parameter value '"
                + paramValue.asText()
                + "' for parameter '"
                + paramName
                + "' has an invalid type '"
                + describeJsonType(paramType)
                + "' for the given job type"
                + " '"
                + jobType
                + "'.");
  }

  @Test
  void testInvalidJobParameterValue() throws JsonProcessingException {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var result = createOrReuseDraftJob(userId, session, jobType);
    var jobId = result.jobId();

    var paramName = "solvingMethod";
    var paramValue = MAPPER.readTree("\"wrong_param_value\"");
    var params = Map.of(paramName, paramValue);
    assertThatExceptionOfType(InvalidJobParameterValueException.class)
        .isThrownBy(() -> jobParamsService.mergeParams(userId, jobId, params))
        .withMessage(
            "Invalid value '"
                + paramValue.asText()
                + "' of parameter '"
                + paramName
                + "' for the given job type '"
                + jobType
                + "'.");
  }

  @Test
  void testNullJobParameterValue() {
    var session = newSessionId();
    var jobType = "solving_slae";
    var userId = userLookupService.findUserId(GUEST);
    var result = createOrReuseDraftJob(userId, session, jobType);
    var jobId = result.jobId();

    var paramName = "solvingMethod";
    var params = new HashMap<String, JsonNode>();
    params.put(paramName, null);
    assertThatExceptionOfType(JobParameterIsNullOrBlankException.class)
        .isThrownBy(() -> jobParamsService.mergeParams(userId, jobId, params))
        .withMessage("Parameter '" + paramName + "' has null value.");
  }

  private static String newSessionId() {
    return "session-" + UUID.randomUUID();
  }

  private JobCreationResult createOrReuseDraftJob(long user, String session, String jobType) {
    return jobCreationService.createOrReuseDraftJob(user, session, jobType);
  }
}

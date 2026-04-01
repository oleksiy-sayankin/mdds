/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.common.util.CommonHelper.findFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.mdds.common.util.HttpTestClient;
import com.mdds.common.util.JsonHelper;
import com.mdds.dto.CreateJobRequestDTO;
import com.mdds.dto.ErrorResponseDTO;
import com.mdds.dto.JobIdResponseDTO;
import com.mdds.dto.ResultDTO;
import com.mdds.grpc.solver.JobStatus;
import com.mdds.server.jpa.JobsRepository;
import com.mdds.storage.redis.RedisDataStorage;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import redis.embedded.RedisServer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.utils.AttributeMap;

@Slf4j
@SpringBootTest(
    classes = ServerApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TestServerApplication {
  @Autowired private JobsRepository jobsRepository;
  @LocalServerPort private int port;

  private static final String HOST = "localhost";
  private static final int REDIS_PORT = findFreePort();
  private static final RedisServer redisServer;
  private static final String DB_NAME = "testdb";
  private static final String USER_NAME = "testuser";
  private static final String PASSWORD = "testpass";

  private static int s3Port;
  private static final String BUCKET = "test-bucket";
  private static final Region REGION = Region.US_EAST_1;
  private static final String ACCESS = "dummy_access_key_id";
  private static final String SECRET = "dummy_secret_key";

  private static final RabbitMQContainer rabbitMq;
  private static final MySQLContainer<?> mysql;
  private static final S3MockContainer s3mock;
  private static final PostgreSQLContainer<?> postgres;

  static {
    rabbitMq =
        new RabbitMQContainer("rabbitmq:3.12-management")
            .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
            .withExposedPorts(5672, 15672)
            .waitingFor(Wait.forListeningPort())
            .withStartupTimeout(Duration.ofSeconds(30));

    mysql =
        new MySQLContainer<>("mysql:8.4.6")
            .withDatabaseName(DB_NAME)
            .withUsername(USER_NAME)
            .withPassword(PASSWORD);

    postgres =
        new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("mdds")
            .withUsername("mdds")
            .withPassword("mdds123");

    s3mock =
        new S3MockContainer("latest")
            .withExposedPorts(9090)
            .waitingFor(Wait.forListeningPort())
            .withStartupTimeout(Duration.ofSeconds(30));
    rabbitMq.start();
    log.info("RabbitMq container is ready {}:{}", rabbitMq.getHost(), rabbitMq.getAmqpPort());
    mysql.start();
    log.info("MySql container is ready {}", mysql.getJdbcUrl());
    postgres.start();
    log.info("Postgres container is ready {}", postgres.getJdbcUrl());
    s3mock.start();
    log.info("S3 container is ready {}:{}", s3mock.getHost(), s3mock.getHttpServerPort());
    try {
      redisServer = new RedisServer(REDIS_PORT);
      redisServer.start();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
    log.info("Embedded Redis Server is ready {}:{}", HOST, REDIS_PORT);
  }

  @DynamicPropertySource
  static void initProps(DynamicPropertyRegistry registry) {
    registry.add("mdds.rabbitmq.host", rabbitMq::getHost);
    registry.add("mdds.rabbitmq.port", rabbitMq::getAmqpPort);
    registry.add("mdds.rabbitmq.user", rabbitMq::getAdminUsername);
    registry.add("mdds.rabbitmq.password", rabbitMq::getAdminPassword);
    registry.add("mdds.redis.host", () -> "localhost");
    registry.add("mdds.redis.port", () -> String.valueOf(REDIS_PORT));
    registry.add("spring.datasource.url", postgres::getJdbcUrl);
    registry.add("spring.datasource.username", postgres::getUsername);
    registry.add("spring.datasource.password", postgres::getPassword);
  }

  @BeforeAll
  static void startServer() throws SQLException {
    createMySqlTestData();
    s3Port = s3mock.getMappedPort(9090);
    createS3TestData();
  }

  @AfterAll
  static void stopServer() throws IOException {
    redisServer.stop();
    mysql.stop();
    s3mock.stop();
    rabbitMq.stop();
    postgres.stop();
  }

  @Test
  void testRootReturnsIndexHtml() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var response = http.get("/");
    assertThat(response.statusCode()).isEqualTo(HttpURLConnection.HTTP_OK);
    assertThat(response.body()).contains("<html");
  }

  @Test
  void testHealthReturnsStatusOk() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var response = http.get("/health");
    assertThat(response.statusCode()).isEqualTo(HttpURLConnection.HTTP_OK);
  }

  @Test
  void testNoResultForJobId() throws IOException, InterruptedException {
    var jobId = "wrong_job_id";
    // Request result using endpoint
    var http = new HttpTestClient(HOST, port);
    var response = http.get("/result/" + jobId);
    assertThat(response.statusCode()).isEqualTo(404);
    var body = JsonHelper.fromJson(response.body(), ErrorResponseDTO.class);
    assertThat(body.message()).isEqualTo("No result found for " + jobId);
  }

  @Test
  void testResultReturnsDataFromDataStorage() throws IOException, InterruptedException {
    var jobId = "test_job_id";
    // Create Result and put it to storage manually
    var expected = new ResultDTO();
    expected.setJobId(jobId);
    expected.setDateTimeJobStarted(Instant.now());
    expected.setDateTimeJobEnded(Instant.now());
    expected.setJobStatus(JobStatus.DONE);
    expected.setProgress(100);
    expected.setSolution(new double[] {81.1, 82.2, 37.3, 45.497});

    // We expect Redis service is up and running here
    try (var storage = new RedisDataStorage(HOST, REDIS_PORT)) {
      storage.put(jobId, expected);
      // Test that data is in data storage
      var actual = storage.get(jobId, ResultDTO.class);
      assertThat(actual.isPresent() ? actual.get() : actual).isEqualTo(expected);
    }

    // Request result using endpoint
    var http = new HttpTestClient(HOST, port);
    var response = http.get("/result/" + jobId);
    var actual = JsonHelper.fromJson(response.body(), ResultDTO.class);
    assertThat(actual).isEqualTo(expected);
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
    var session = newSessionId();
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
                session),
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
                session),
            new CreateJobRequestDTO(otherJobType));
    assertThat(response.statusCode()).isEqualTo(HttpStatus.CONFLICT.value());
    assertThat(message(response.body()))
        .isEqualTo(
            "A draft job already exists for upload session id '"
                + session
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
    var session = newSessionId();
    var response =
        http.post(
            "/jobs",
            Map.of(
                "Content-Type",
                "application/xml",
                "X-MDDS-User-Login",
                "guest",
                "X-MDDS-Upload-Session-Id",
                session),
            new CreateJobRequestDTO("solving_slae"));
    assertThat(response.statusCode()).isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE.value());
  }

  @Test
  void testCreateOrReuseDraftJobReturnsErrorForMissingContentType()
      throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    var session = newSessionId();
    var response =
        http.post(
            "/jobs",
            Map.of("X-MDDS-User-Login", "guest", "X-MDDS-Upload-Session-Id", session),
            new CreateJobRequestDTO("solving_slae"));
    assertThat(response.statusCode()).isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE.value());
  }

  @Test
  void testSolveHttpRequestDataSource() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    Map<String, Object> params = new HashMap<>();
    params.put(
        "matrix",
        List.of(List.of(1.3, 2.4, 3.1), List.of(4.77, 5.2321, 6.32), List.of(7.23, 8.43, 9.4343)));
    params.put("rhs", List.of(1.3, 2.2, 3.7));
    var response = http.postSolve("http_request", "numpy_exact_solver", params);
    assertThat(response.statusCode()).isEqualTo(200);
    var contentType = response.headers().firstValue("Content-Type").orElse("");
    assertThat(contentType).contains("application/json");
    var json = response.body();
    var id = JsonHelper.fromJson(json, JobIdResponseDTO.class).getJobId();
    assertThat(id).isNotNull();
  }

  @Test
  void testSolveMysqlDataSource() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    Map<String, Object> params = new HashMap<>();
    params.put("mysql.url", mysql.getJdbcUrl());
    params.put("mysql.user", USER_NAME);
    params.put("mysql.password", PASSWORD);
    params.put("mysql.db.name", DB_NAME);
    params.put("mysql.matrix.table.name", "MATRIX_TABLE");
    params.put("mysql.matrix.json.field.name", "JSON_FIELD");
    params.put("mysql.matrix.primary.key.field.name", "ID");
    params.put("mysql.matrix.primary.key.field.value", "1");
    params.put("mysql.rhs.table.name", "RHS_TABLE");
    params.put("mysql.rhs.json.field.name", "JSON_FIELD");
    params.put("mysql.rhs.primary.key.field.name", "ID");
    params.put("mysql.rhs.primary.key.field.value", "1");
    var response = http.postSolve("mysql", "numpy_exact_solver", params);
    var json = response.body();
    var id = JsonHelper.fromJson(json, JobIdResponseDTO.class).getJobId();
    assertThat(id).isNotNull();
  }

  private static void createMySqlTestData() throws SQLException {
    var jdbcUrl = mysql.getJdbcUrl();
    try (var connection = DriverManager.getConnection(jdbcUrl, USER_NAME, PASSWORD);
        var stmt = connection.createStatement()) {
      stmt.execute("USE " + DB_NAME);
      stmt.execute("CREATE TABLE MATRIX_TABLE (ID INT PRIMARY KEY, JSON_FIELD VARCHAR(255))");
      stmt.execute("CREATE TABLE RHS_TABLE (ID INT PRIMARY KEY, JSON_FIELD VARCHAR(255))");
      stmt.execute(
          "INSERT INTO MATRIX_TABLE (ID, JSON_FIELD) VALUES (1,"
              + " '[[3.4,5.5,2.2],[1.2,5.5,8.1],[3.4,8.6,9.4]]')");
      stmt.execute("INSERT INTO RHS_TABLE (ID, JSON_FIELD) VALUES (1,'[4.7,1.5,5.3]')");
    }
  }

  @Test
  void testSolveS3DataSource() throws IOException, InterruptedException {
    var http = new HttpTestClient(HOST, port);
    Map<String, Object> params = new HashMap<>();
    var endpoint = "http://localhost:" + s3Port;
    params.put("aws.bucket.name", BUCKET);
    params.put("aws.use.endpoint.url", "true");
    params.put("aws.endpoint.url", endpoint);
    params.put("aws.region", REGION.id());
    params.put("aws.access.key.id", ACCESS);
    params.put("aws.secret.access.key", SECRET);
    params.put("aws.matrix.key", "matrix.json");
    params.put("aws.rhs.key", "rhs.json");
    params.put("aws.path.style.access.enabled", "true");
    var response = http.postSolve("s3", "numpy_exact_solver", params);
    var json = response.body();
    var id = JsonHelper.fromJson(json, JobIdResponseDTO.class).getJobId();
    assertThat(id).isNotNull();
  }

  private static void createS3TestData() {
    var endpoint = s3mock.getHttpEndpoint();
    var serviceConfig = S3Configuration.builder().pathStyleAccessEnabled(true).build();
    var httpClient =
        UrlConnectionHttpClient.builder()
            .buildWithDefaults(
                AttributeMap.builder().put(TRUST_ALL_CERTIFICATES, Boolean.TRUE).build());

    try (var s3Client =
        S3Client.builder()
            .endpointOverride(URI.create(endpoint))
            .serviceConfiguration(serviceConfig)
            .httpClient(httpClient)
            .region(REGION)
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS, SECRET)))
            .build()) {
      var matrix = "[[1.2,3.343,543],[4.32,243.3,2.232],[7.32,32.32,432.1]]";
      var rhs = "[4.3,6.2,3.3]";
      s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
      var matrixByteBuffer = ByteBuffer.wrap((matrix.getBytes(UTF_8)));
      var rhsByteBuffer = ByteBuffer.wrap((rhs.getBytes(UTF_8)));
      s3Client.putObject(
          PutObjectRequest.builder().bucket(BUCKET).key("matrix.json").build(),
          RequestBody.fromByteBuffer(matrixByteBuffer));
      s3Client.putObject(
          PutObjectRequest.builder().bucket(BUCKET).key("rhs.json").build(),
          RequestBody.fromByteBuffer(rhsByteBuffer));
    }
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

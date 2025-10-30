/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.mdds.common.util.JsonHelper;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.SlaeSolver;
import com.mdds.dto.TaskIdResponseDTO;
import com.mdds.dto.TaskStatus;
import com.rabbitmq.client.ConnectionFactory;
import io.grpc.ManagedChannelBuilder;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
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
@Testcontainers
class TestContainers {
  private static final String GRPC_SERVER_HOST = "grpcserver";
  private static final int GRPC_SERVER_PORT = 50051;

  private static final String MDDS_EXECUTOR_HOST = "executor";
  private static final int MDDS_EXECUTOR_PORT = 35232;

  private static final String MDDS_WEB_SERVER_HOST = "webserver";
  private static final int MDDS_WEB_SERVER_PORT = 8000;

  private static final String MDDS_RESULT_CONSUMER_HOST = "resultconsumer";
  private static final int MDDS_RESULT_CONSUMER_PORT = 8863;

  private static final String REDIS_HOST = "redis";
  private static final int REDIS_PORT = 6379;

  private static final String RABBITMQ_HOST = "rabbitmq";
  private static final int RABBITMQ_PORT = 5672;

  private static final String MDDS_GRPC_CORE = "mdds_grpc_core";
  private static final String MDDS_HOME = File.separator + "opt" + File.separator + "mdds";
  private static final String GRPC_CORE_WORK_DIR = MDDS_HOME + File.separator + MDDS_GRPC_CORE;

  private static final String DB_NAME = "testdb";
  private static final String USER_NAME = "testuser";
  private static final String PASSWORD = "testpass";
  private static final String MYSQL_HOST = "mysqlhost";
  private static final String MYSQL_URL =
      "jdbc:mysql://" + MYSQL_HOST + ":" + MySQLContainer.MYSQL_PORT + "/" + DB_NAME;

  private static final String S_3_HOST = "s3host";
  private static final int S_3_PORT = 9090;
  private static final String S_3_BUCKET = "test-bucket";
  private static final Region S_3_REGION = Region.US_EAST_1;
  private static final String S_3_ACCESS_KEY_ID = "dummy_access_key_id";
  private static final String S_3_SECRET_ACCESS_KEY = "dummy_secret_key";
  private static final String MATRIX_KEY = "matrix.json";
  private static final String RHS_KEY = "rhs.json";
  private static final boolean PATH_STYLE_ACCESS_ENABLED = true;

  private static final Network sharedNetwork = Network.newNetwork();

  @Container
  private static final RabbitMQContainer rabbitMq =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withNetwork(sharedNetwork)
          .withNetworkAliases(RABBITMQ_HOST)
          .withExposedPorts(RABBITMQ_PORT);

  @SuppressWarnings("resource")
  @Container
  private static final GenericContainer<?> redis =
      new GenericContainer<>(DockerImageName.parse("redis:7.4"))
          .withNetwork(sharedNetwork)
          .withNetworkAliases(REDIS_HOST)
          .withExposedPorts(REDIS_PORT);

  @SuppressWarnings("resource")
  @Container
  private static final GenericContainer<?> grpcServer =
      new GenericContainer<>(DockerImageName.parse("mddsproject/grpc-server:0.1.0"))
          .withNetwork(sharedNetwork)
          .withNetworkAliases(GRPC_SERVER_HOST)
          .withEnv("MDDS_EXECUTOR_GRPC_SERVER_HOST", GRPC_SERVER_HOST)
          .withEnv("MDDS_EXECUTOR_GRPC_SERVER_PORT", String.valueOf(GRPC_SERVER_PORT))
          .withWorkingDirectory(GRPC_CORE_WORK_DIR)
          .withExposedPorts(GRPC_SERVER_PORT);

  @SuppressWarnings("resource")
  @Container
  private static final GenericContainer<?> executor =
      new GenericContainer<>(DockerImageName.parse("mddsproject/executor:0.1.0"))
          .withNetwork(sharedNetwork)
          .withNetworkAliases(MDDS_EXECUTOR_HOST)
          .withEnv("MDDS_EXECUTOR_HOST", MDDS_EXECUTOR_HOST)
          .withEnv("MDDS_EXECUTOR_PORT", String.valueOf(MDDS_EXECUTOR_PORT))
          .withEnv("MDDS_EXECUTOR_GRPC_SERVER_HOST", GRPC_SERVER_HOST)
          .withEnv("MDDS_EXECUTOR_GRPC_SERVER_PORT", String.valueOf(GRPC_SERVER_PORT))
          .withEnv("RABBITMQ_HOST", RABBITMQ_HOST)
          .withEnv("RABBITMQ_PORT", String.valueOf(RABBITMQ_PORT))
          .withExposedPorts(MDDS_EXECUTOR_PORT);

  @SuppressWarnings("resource")
  @Container
  private static final GenericContainer<?> webServer =
      new GenericContainer<>(DockerImageName.parse("mddsproject/web-server:0.1.0"))
          .withNetwork(sharedNetwork)
          .withNetworkAliases(MDDS_WEB_SERVER_HOST)
          .withEnv("MDDS_SERVER_HOST", MDDS_WEB_SERVER_HOST)
          .withEnv("MDDS_SERVER_PORT", String.valueOf(MDDS_WEB_SERVER_PORT))
          .withEnv("RABBITMQ_HOST", RABBITMQ_HOST)
          .withEnv("RABBITMQ_PORT", String.valueOf(RABBITMQ_PORT))
          .withEnv("REDIS_HOST", REDIS_HOST)
          .withEnv("REDIS_PORT", String.valueOf(REDIS_PORT))
          .withExposedPorts(MDDS_WEB_SERVER_PORT);

  @SuppressWarnings("resource")
  @Container
  private static final GenericContainer<?> resultConsumer =
      new GenericContainer<>(DockerImageName.parse("mddsproject/result-consumer:0.1.0"))
          .withNetwork(sharedNetwork)
          .withNetworkAliases(MDDS_RESULT_CONSUMER_HOST)
          .withEnv("RABBITMQ_HOST", RABBITMQ_HOST)
          .withEnv("RABBITMQ_PORT", String.valueOf(RABBITMQ_PORT))
          .withEnv("REDIS_HOST", REDIS_HOST)
          .withEnv("REDIS_PORT", String.valueOf(REDIS_PORT))
          .withExposedPorts(MDDS_RESULT_CONSUMER_PORT);

  @SuppressWarnings("resource")
  @Container
  private static final MySQLContainer<?> MY_SQL_CONTAINER =
      new MySQLContainer<>("mysql:8.4.6")
          .withDatabaseName(DB_NAME)
          .withUsername(USER_NAME)
          .withPassword(PASSWORD)
          .withNetwork(sharedNetwork)
          .withNetworkAliases(MYSQL_HOST);

  @Container
  private static final S3MockContainer S_3_MOCK_CONTAINER =
      new S3MockContainer("latest").withNetwork(sharedNetwork).withNetworkAliases(S_3_HOST);

  @BeforeAll
  static void startServer() throws SQLException {
    executor.followOutput(
        outputFrame -> log.info("[EXECUTOR] {}", outputFrame.getUtf8String().trim()));
    webServer.followOutput(
        outputFrame -> log.info("[WEB-SERVER] {}", outputFrame.getUtf8String().trim()));
    resultConsumer.followOutput(
        outputFrame -> log.info("[RESULT-CONSUMER] {}", outputFrame.getUtf8String().trim()));

    // Wait for RabbitMq is ready
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(TestContainers::queueIsReady);
    log.info("RabbitMq container is ready");

    // Wait for Redis is ready
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(TestContainers::redisIsReady);
    log.info("Redis container is ready");

    // Wait for gRPC Server is ready
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(TestContainers::grpcServerIsReady);
    log.info("gRPC Server is ready");

    // Wait for Executor is ready
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(TestContainers::executorIsReady);
    log.info("Executor is ready");

    // Wait for Web Server is ready
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(TestContainers::webServerIsReady);
    log.info("Web Server is ready");

    // Wait for Web Server is ready
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(TestContainers::resultConsumerIsReady);
    log.info("Result Consumer is ready");

    // Wait for MySql Server is ready
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(TestContainers::mysqlIsReady);
    createMySqlTestData();
    log.info("Mysql Container is ready");

    // Wait for s3 Server is ready
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(TestContainers::s3MockIsReady);
    createS3TestData();
    log.info("S3 Container is ready");
  }

  private static Stream<SlaeSolver> solvers() {
    return Stream.of(
        SlaeSolver.NUMPY_EXACT_SOLVER,
        SlaeSolver.NUMPY_PINV_SOLVER,
        SlaeSolver.PETSC_SOLVER,
        SlaeSolver.NUNPY_LSTSQ_SOLVER,
        SlaeSolver.SCIPY_GMERS_SOLVER);
  }

  @ParameterizedTest
  @MethodSource("solvers")
  void commonTestHttpRequestDataSource(SlaeSolver slaeSolver)
      throws IOException, URISyntaxException {

    var uri =
        new AtomicReference<>(
            new URI(
                "http://"
                    + webServer.getHost()
                    + ":"
                    + webServer.getMappedPort(MDDS_WEB_SERVER_PORT)
                    + "/solve"));
    var url = new AtomicReference<>(uri.get().toURL());

    var boundary = "----TestBoundary";
    var connection = new AtomicReference<>((HttpURLConnection) url.get().openConnection());
    connection.get().setDoOutput(true);
    connection.get().setRequestMethod("POST");
    connection
        .get()
        .setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

    try (var output = connection.get().getOutputStream()) {
      var writer = new PrintWriter(new OutputStreamWriter(output, UTF_8), true);

      // add slaeSolvingMethod
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"slaeSolvingMethod\"\r\n\r\n");
      writer.append(slaeSolver.getName()).append("\r\n");
      writer.flush();

      // add dataSourceType
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"dataSourceType\"\r\n\r\n");
      writer.append("http_request").append("\r\n");
      writer.flush();

      // add matrix
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"matrix\"; filename=\"matrix.csv\"\r\n");
      writer.append("Content-Type: text/csv\r\n\r\n");
      writer.flush();
      output.write("1.3,2.4,3.1\n4.77,5.2321,6.32\n7.23,8.43,9.4343\n".getBytes(UTF_8));
      output.write("\r\n".getBytes(UTF_8));
      output.flush();

      // put rhs
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"rhs\"; filename=\"rhs.csv\"\r\n");
      writer.append("Content-Type: text/csv\r\n\r\n");
      writer.flush();
      output.write("1.3\n2.2\n3.7\n".getBytes(UTF_8));
      output.write("\r\n".getBytes(UTF_8));
      output.flush();

      // finish the request
      writer.append("--").append(boundary).append("--").append("\r\n");
      writer.close();
    }

    // Check the answer
    assertEquals(HttpURLConnection.HTTP_OK, connection.get().getResponseCode());
    assertEquals("application/json;charset=ISO-8859-1", connection.get().getContentType());

    TaskIdResponseDTO response;
    try (var reader =
        new BufferedReader(new InputStreamReader(connection.get().getInputStream()))) {
      var body = reader.lines().reduce("", (a, b) -> a + b);
      assertTrue(body.contains("id"));
      response = JsonHelper.fromJson(body, TaskIdResponseDTO.class);
    }
    var id = response.getId();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              uri.set(
                  new URI(
                      "http://"
                          + webServer.getHost()
                          + ":"
                          + webServer.getMappedPort(MDDS_WEB_SERVER_PORT)
                          + "/result/"
                          + id));
              url.set(uri.get().toURL());
              connection.set((HttpURLConnection) url.get().openConnection());
              connection.get().setRequestMethod("GET");

              assertEquals(HttpURLConnection.HTTP_OK, connection.get().getResponseCode());
              assertEquals(
                  "application/json;charset=ISO-8859-1", connection.get().getContentType());

              ResultDTO actualResult;
              try (var reader =
                  new BufferedReader(new InputStreamReader(connection.get().getInputStream()))) {
                var body = reader.lines().reduce("", (a, b) -> a + b);
                actualResult = JsonHelper.fromJson(body, ResultDTO.class);
              }
              assertSame(TaskStatus.DONE, actualResult.getTaskStatus());

              var expectedResult =
                  new double[] {
                    -0.3291566787737896398658, 0.7293212011512698153684, -0.0072474839861680725996
                  };
              var delta = 0.00000001;
              assertArrayEquals(expectedResult, actualResult.getSolution(), delta);
            });
  }

  @ParameterizedTest
  @MethodSource("solvers")
  void commonTestMySqlDataSource(SlaeSolver slaeSolver) throws IOException, URISyntaxException {
    var uri =
        new AtomicReference<>(
            new URI(
                "http://"
                    + webServer.getHost()
                    + ":"
                    + webServer.getMappedPort(MDDS_WEB_SERVER_PORT)
                    + "/solve"));
    var url = new AtomicReference<>(uri.get().toURL());

    var boundary = "----TestBoundary";
    var connection = new AtomicReference<>((HttpURLConnection) url.get().openConnection());
    connection.get().setDoOutput(true);
    connection.get().setRequestMethod("POST");
    connection
        .get()
        .setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

    try (var output = connection.get().getOutputStream()) {
      var writer = new PrintWriter(new OutputStreamWriter(output, UTF_8), true);

      // add slaeSolvingMethod
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"slaeSolvingMethod\"\r\n\r\n");
      writer.append(slaeSolver.getName()).append("\r\n");
      writer.flush();

      // add dataSourceType
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"dataSourceType\"\r\n\r\n");
      writer.append("mysql").append("\r\n");
      writer.flush();

      // add mysqlUrl
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlUrl\"\r\n\r\n");
      writer.append(MYSQL_URL).append("\r\n");
      writer.flush();

      // add mysqlUser
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlUser\"\r\n\r\n");
      writer.append(USER_NAME).append("\r\n");
      writer.flush();

      // add mysqlPassword
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlPassword\"\r\n\r\n");
      writer.append(PASSWORD).append("\r\n");
      writer.flush();

      // add mysqlDbName
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlDbName\"\r\n\r\n");
      writer.append(DB_NAME).append("\r\n");
      writer.flush();

      // add mysqlMatrixTableName
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlMatrixTableName\"\r\n\r\n");
      writer.append("MATRIX_TABLE").append("\r\n");
      writer.flush();

      // add mysqlMatrixJsonFieldName
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlMatrixJsonFieldName\"\r\n\r\n");
      writer.append("JSON_FIELD").append("\r\n");
      writer.flush();

      // add mysqlMatrixPrimaryKeyFieldName
      writer.append("--").append(boundary).append("\r\n");
      writer.append(
          "Content-Disposition: form-data; name=\"mysqlMatrixPrimaryKeyFieldName\"\r\n\r\n");
      writer.append("ID").append("\r\n");
      writer.flush();

      // add mysqlMatrixPrimaryKeyFieldValue
      writer.append("--").append(boundary).append("\r\n");
      writer.append(
          "Content-Disposition: form-data; name=\"mysqlMatrixPrimaryKeyFieldValue\"\r\n\r\n");
      writer.append("1").append("\r\n");
      writer.flush();

      // add mysqlRhsTableName
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlRhsTableName\"\r\n\r\n");
      writer.append("RHS_TABLE").append("\r\n");
      writer.flush();

      // add mysqlRhsJsonFieldName
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlRhsJsonFieldName\"\r\n\r\n");
      writer.append("JSON_FIELD").append("\r\n");
      writer.flush();

      // add mysqlRhsPrimaryKeyFieldName
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlRhsPrimaryKeyFieldName\"\r\n\r\n");
      writer.append("ID").append("\r\n");
      writer.flush();

      // add mysqlRhsPrimaryKeyFieldValue
      writer.append("--").append(boundary).append("\r\n");
      writer.append(
          "Content-Disposition: form-data; name=\"mysqlRhsPrimaryKeyFieldValue\"\r\n\r\n");
      writer.append("1").append("\r\n");
      writer.flush();

      // finish the request
      writer.append("--").append(boundary).append("--").append("\r\n");
      writer.close();
    }

    // Check the answer
    assertEquals(HttpURLConnection.HTTP_OK, connection.get().getResponseCode());
    assertEquals("application/json;charset=ISO-8859-1", connection.get().getContentType());

    TaskIdResponseDTO response;
    try (var reader =
        new BufferedReader(new InputStreamReader(connection.get().getInputStream()))) {
      var body = reader.lines().reduce("", (a, b) -> a + b);
      assertTrue(body.contains("id"));
      response = JsonHelper.fromJson(body, TaskIdResponseDTO.class);
    }
    var id = response.getId();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              uri.set(
                  new URI(
                      "http://"
                          + webServer.getHost()
                          + ":"
                          + webServer.getMappedPort(MDDS_WEB_SERVER_PORT)
                          + "/result/"
                          + id));
              url.set(uri.get().toURL());
              connection.set((HttpURLConnection) url.get().openConnection());
              connection.get().setRequestMethod("GET");

              assertEquals(HttpURLConnection.HTTP_OK, connection.get().getResponseCode());
              assertEquals(
                  "application/json;charset=ISO-8859-1", connection.get().getContentType());

              ResultDTO actualResult;
              try (var reader =
                  new BufferedReader(new InputStreamReader(connection.get().getInputStream()))) {
                var body = reader.lines().reduce("", (a, b) -> a + b);
                actualResult = JsonHelper.fromJson(body, ResultDTO.class);
              }
              assertSame(TaskStatus.DONE, actualResult.getTaskStatus());

              var expectedResult =
                  new double[] {
                    3.8673716012084592145015, -1.8960725075528700906344, 0.8996978851963746223565
                  };
              var delta = 0.00000001;
              assertArrayEquals(expectedResult, actualResult.getSolution(), delta);
            });
  }

  private static void createMySqlTestData() throws SQLException {
    var jdbcUrl = MY_SQL_CONTAINER.getJdbcUrl();
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

  private static boolean mysqlIsReady() {
    try (var conn =
        DriverManager.getConnection(MY_SQL_CONTAINER.getJdbcUrl(), USER_NAME, PASSWORD)) {
      return conn.isValid(2);
    } catch (SQLException e) {
      return false;
    }
  }

  @ParameterizedTest
  @MethodSource("solvers")
  void commonTestS3DataSource(SlaeSolver slaeSolver) throws IOException, URISyntaxException {
    var uri =
        new AtomicReference<>(
            new URI(
                "http://"
                    + webServer.getHost()
                    + ":"
                    + webServer.getMappedPort(MDDS_WEB_SERVER_PORT)
                    + "/solve"));
    var url = new AtomicReference<>(uri.get().toURL());

    var boundary = "----TestBoundary";
    var connection = new AtomicReference<>((HttpURLConnection) url.get().openConnection());
    connection.get().setDoOutput(true);
    connection.get().setRequestMethod("POST");
    connection
        .get()
        .setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

    try (var output = connection.get().getOutputStream()) {
      var writer = new PrintWriter(new OutputStreamWriter(output, UTF_8), true);

      // add slaeSolvingMethod
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"slaeSolvingMethod\"\r\n\r\n");
      writer.append(slaeSolver.getName()).append("\r\n");
      writer.flush();

      // add dataSourceType
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"dataSourceType\"\r\n\r\n");
      writer.append("s3").append("\r\n");
      writer.flush();

      // add awsBucketName
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"awsBucketName\"\r\n\r\n");
      writer.append(S_3_BUCKET).append("\r\n");
      writer.flush();

      // add awsUseEndPointUrl
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"awsUseEndPointUrl\"\r\n\r\n");
      writer.append("true").append("\r\n");
      writer.flush();

      // add awsEndPointUrl
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"awsEndPointUrl\"\r\n\r\n");
      writer.append("http://" + S_3_HOST + ":" + S_3_PORT).append("\r\n");
      writer.flush();

      // add awsRegion
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"awsRegion\"\r\n\r\n");
      writer.append(S_3_REGION.id()).append("\r\n");
      writer.flush();

      // add awsAccessKeyId
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"awsAccessKeyId\"\r\n\r\n");
      writer.append(S_3_ACCESS_KEY_ID).append("\r\n");
      writer.flush();

      // add awsSecretAccessKey
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"awsSecretAccessKey\"\r\n\r\n");
      writer.append(S_3_SECRET_ACCESS_KEY).append("\r\n");
      writer.flush();

      // add awsMatrixKey
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"awsMatrixKey\"\r\n\r\n");
      writer.append(MATRIX_KEY).append("\r\n");
      writer.flush();

      // add awsRhsKey
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"awsRhsKey\"\r\n\r\n");
      writer.append(RHS_KEY).append("\r\n");
      writer.flush();

      // add awsPathStyleAccessEnabled
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"awsPathStyleAccessEnabled\"\r\n\r\n");
      writer.append("true").append("\r\n");
      writer.flush();

      // finish the request
      writer.append("--").append(boundary).append("--").append("\r\n");
      writer.close();
    }

    // Check the answer
    assertEquals(HttpURLConnection.HTTP_OK, connection.get().getResponseCode());
    assertEquals("application/json;charset=ISO-8859-1", connection.get().getContentType());

    TaskIdResponseDTO response;
    try (var reader =
        new BufferedReader(new InputStreamReader(connection.get().getInputStream()))) {
      var body = reader.lines().reduce("", (a, b) -> a + b);
      assertTrue(body.contains("id"));
      response = JsonHelper.fromJson(body, TaskIdResponseDTO.class);
    }
    var id = response.getId();

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              uri.set(
                  new URI(
                      "http://"
                          + webServer.getHost()
                          + ":"
                          + webServer.getMappedPort(MDDS_WEB_SERVER_PORT)
                          + "/result/"
                          + id));
              url.set(uri.get().toURL());
              connection.set((HttpURLConnection) url.get().openConnection());
              connection.get().setRequestMethod("GET");

              assertEquals(HttpURLConnection.HTTP_OK, connection.get().getResponseCode());
              assertEquals(
                  "application/json;charset=ISO-8859-1", connection.get().getContentType());

              ResultDTO actualResult;
              try (var reader =
                  new BufferedReader(new InputStreamReader(connection.get().getInputStream()))) {
                var body = reader.lines().reduce("", (a, b) -> a + b);
                actualResult = JsonHelper.fromJson(body, ResultDTO.class);
              }
              assertSame(TaskStatus.DONE, actualResult.getTaskStatus());

              var expectedResult =
                  new double[] {
                    -0.1499382089687040253643, 0.0280711223847708241758, 0.0080775029256540625510
                  };
              var delta = 0.00000001;
              assertArrayEquals(expectedResult, actualResult.getSolution(), delta);
            });
  }

  private static void createS3TestData() {
    var endpoint = S_3_MOCK_CONTAINER.getHttpEndpoint();
    var serviceConfig =
        S3Configuration.builder().pathStyleAccessEnabled(PATH_STYLE_ACCESS_ENABLED).build();
    var httpClient =
        UrlConnectionHttpClient.builder()
            .buildWithDefaults(
                AttributeMap.builder().put(TRUST_ALL_CERTIFICATES, Boolean.TRUE).build());

    try (var s3Client =
        S3Client.builder()
            .endpointOverride(URI.create(endpoint))
            .serviceConfiguration(serviceConfig)
            .httpClient(httpClient)
            .region(S_3_REGION)
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(S_3_ACCESS_KEY_ID, S_3_SECRET_ACCESS_KEY)))
            .build()) {
      var matrix = "[[1.2,3.343,543],[4.32,243.3,2.232],[7.32,32.32,432.1]]";
      var rhs = "[4.3,6.2,3.3]";
      s3Client.createBucket(CreateBucketRequest.builder().bucket(S_3_BUCKET).build());
      var matrixByteBuffer = ByteBuffer.wrap((matrix.getBytes(UTF_8)));
      var rhsByteBuffer = ByteBuffer.wrap((rhs.getBytes(UTF_8)));
      s3Client.putObject(
          PutObjectRequest.builder().bucket(S_3_BUCKET).key(MATRIX_KEY).build(),
          RequestBody.fromByteBuffer(matrixByteBuffer));
      s3Client.putObject(
          PutObjectRequest.builder().bucket(S_3_BUCKET).key(RHS_KEY).build(),
          RequestBody.fromByteBuffer(rhsByteBuffer));
    }
  }

  private static boolean s3MockIsReady() {
    try {
      var endpoint = S_3_MOCK_CONTAINER.getHttpEndpoint();
      var serviceConfig =
          S3Configuration.builder().pathStyleAccessEnabled(PATH_STYLE_ACCESS_ENABLED).build();
      var httpClient =
          UrlConnectionHttpClient.builder()
              .buildWithDefaults(
                  AttributeMap.builder().put(TRUST_ALL_CERTIFICATES, Boolean.TRUE).build());

      try (var s3Client =
          S3Client.builder()
              .endpointOverride(URI.create(endpoint))
              .serviceConfiguration(serviceConfig)
              .httpClient(httpClient)
              .region(S_3_REGION)
              .credentialsProvider(
                  StaticCredentialsProvider.create(
                      AwsBasicCredentials.create(S_3_ACCESS_KEY_ID, S_3_SECRET_ACCESS_KEY)))
              .build()) {
        s3Client.listBuckets();
        return true;
      } catch (Exception e) {
        return false;
      }
    } catch (Exception e) {
      return false;
    }
  }

  private static boolean redisIsReady() throws IOException, InterruptedException {
    var result = redis.execInContainer("redis-cli", "ping");
    return result.getExitCode() == 0 && "PONG".equals(result.getStdout().trim());
  }

  private static boolean resultConsumerIsReady() throws IOException, URISyntaxException {
    return checkHealth(resultConsumer, MDDS_RESULT_CONSUMER_PORT);
  }

  private static boolean webServerIsReady() throws IOException, URISyntaxException {
    return checkHealth(webServer, MDDS_WEB_SERVER_PORT);
  }

  private static boolean executorIsReady() throws IOException, URISyntaxException {
    return checkHealth(executor, MDDS_EXECUTOR_PORT);
  }

  private static boolean checkHealth(GenericContainer<?> container, int internalPort)
      throws URISyntaxException, IOException {
    var host = container.getHost();
    var port = container.getMappedPort(internalPort);
    var uri = new URI("http://" + host + ":" + port + "/health");
    var url = uri.toURL();
    var connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    return HttpURLConnection.HTTP_OK == connection.getResponseCode();
  }

  private static boolean queueIsReady() throws IOException, TimeoutException {
    try (var connection =
        createConnectionFactory(
                rabbitMq.getHost(),
                rabbitMq.getAmqpPort(),
                rabbitMq.getAdminUsername(),
                rabbitMq.getAdminPassword())
            .newConnection()) {
      return connection.isOpen();
    }
  }

  private static boolean grpcServerIsReady() {
    var channel =
        ManagedChannelBuilder.forAddress(
                grpcServer.getHost(), grpcServer.getMappedPort(GRPC_SERVER_PORT))
            .usePlaintext()
            .build();
    var stub = HealthGrpc.newBlockingStub(channel);
    var response = stub.check(HealthCheckRequest.newBuilder().build());
    var result = HealthCheckResponse.ServingStatus.SERVING.equals(response.getStatus());
    channel.shutdownNow();
    return result;
  }

  private static ConnectionFactory createConnectionFactory(
      String host, int port, String user, String password) {
    var factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setPort(port);
    factory.setUsername(user);
    factory.setPassword(password);
    return factory;
  }
}

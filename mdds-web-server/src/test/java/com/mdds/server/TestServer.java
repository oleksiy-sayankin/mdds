/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.common.util.CommonHelper.findFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.mdds.common.util.JsonHelper;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.TaskStatus;
import com.mdds.storage.DataStorageFactory;
import com.mdds.storage.redis.RedisConfFactory;
import com.rabbitmq.client.ConnectionFactory;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
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

@Testcontainers
class TestServer {
  private static Tomcat tomcat;
  private static final String MDDS_SERVER_HOST =
      ServerConfFactory.fromEnvOrDefaultProperties().host();
  private static final int MDDS_SERVER_PORT = findFreePort();
  private static final String MDDS_SERVER_WEB_APPLICATION_LOCATION =
      ServerConfFactory.fromEnvOrDefaultProperties().webappDirLocation();
  private static final int REDIS_SERVER_PORT = findFreePort();
  private static RedisServer redisServer;
  private static final String DB_NAME = "testdb";
  private static final String USER_NAME = "testuser";
  private static final String PASSWORD = "testpass";

  private static final String S_3_HOST = "localhost";
  private static int s3Port;
  private static final String S_3_BUCKET = "test-bucket";
  private static final Region S_3_REGION = Region.US_EAST_1;
  private static final String S_3_ACCESS_KEY_ID = "dummy_access_key_id";
  private static final String S_3_SECRET_ACCESS_KEY = "dummy_secret_key";
  private static final String MATRIX_KEY = "matrix.json";
  private static final String RHS_KEY = "rhs.json";
  private static final boolean PATH_STYLE_ACCESS_ENABLED = true;

  @Container
  private static final RabbitMQContainer rabbitMq =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672);

  @Container
  private static final MySQLContainer<?> MY_SQL_CONTAINER =
      new MySQLContainer<>("mysql:8.4.6")
          .withDatabaseName(DB_NAME)
          .withUsername(USER_NAME)
          .withPassword(PASSWORD);

  @Container
  private static final S3MockContainer S_3_MOCK_CONTAINER = new S3MockContainer("latest");

  @BeforeAll
  static void startServer() throws LifecycleException, IOException, SQLException {
    redisServer = new RedisServer(REDIS_SERVER_PORT);
    redisServer.start();

    // Wait for RabbitMq is ready
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(TestServer::queueIsReady);

    System.setProperty("redis.host", "localhost");
    System.setProperty("redis.port", String.valueOf(REDIS_SERVER_PORT));
    System.setProperty("rabbitmq.host", rabbitMq.getHost());
    System.setProperty("rabbitmq.port", String.valueOf(rabbitMq.getAmqpPort()));
    System.setProperty("rabbitmq.user", rabbitMq.getAdminUsername());
    System.setProperty("rabbitmq.password", rabbitMq.getAdminPassword());

    createMySqlTestData();
    s3Port = S_3_MOCK_CONTAINER.getMappedPort(9090);
    createS3TestData();

    tomcat = Server.start(MDDS_SERVER_HOST, MDDS_SERVER_PORT, MDDS_SERVER_WEB_APPLICATION_LOCATION);
  }

  @AfterAll
  static void stopServer() throws LifecycleException, IOException {
    if (tomcat != null) {
      tomcat.stop();
      tomcat.destroy();
    }
    if (redisServer != null) {
      redisServer.stop();
    }
  }

  @Test
  void testRootReturnsIndexHtml() throws Exception {
    var uri = new URI("http://" + MDDS_SERVER_HOST + ":" + MDDS_SERVER_PORT);
    var url = uri.toURL();
    var connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");

    assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    assertEquals("text/html", connection.getContentType());

    try (var reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
      String body = reader.lines().reduce("", (a, b) -> a + b);
      assertTrue(body.contains("<html"));
    }
  }

  @Test
  void testHealthReturnsStatusOk() throws URISyntaxException, IOException {
    var uri = new URI("http://" + MDDS_SERVER_HOST + ":" + MDDS_SERVER_PORT + "/health");
    var url = uri.toURL();
    var connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
  }

  @Test
  void testResultReturnsDataFromDataStorage() throws URISyntaxException, IOException {
    var taskId = "test_task_id";
    // Create Result and put it to storage manually
    var expectedResult = new ResultDTO();
    expectedResult.setTaskId(taskId);
    expectedResult.setDateTimeTaskCreated(Instant.now());
    expectedResult.setDateTimeTaskFinished(Instant.now());
    expectedResult.setTaskStatus(TaskStatus.DONE);
    expectedResult.setProgress(100);
    expectedResult.setSolution(new double[] {81.1, 82.2, 37.3, 45.497});

    // We expect Redis service is up and running here
    try (var storage =
        DataStorageFactory.createRedis(RedisConfFactory.fromEnvOrDefaultProperties())) {
      storage.put(taskId, expectedResult);
      // Test that data is in data storage
      var actualResult = storage.get(taskId, ResultDTO.class);
      Assertions.assertEquals(
          expectedResult, actualResult.isPresent() ? actualResult.get() : actualResult);
    }

    // Request result using endpoint
    var uri = new URI("http://" + MDDS_SERVER_HOST + ":" + MDDS_SERVER_PORT + "/result/" + taskId);
    var url = uri.toURL();
    var connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    assertEquals("application/json;charset=ISO-8859-1", connection.getContentType());

    // Read result from the response.
    var sb = new StringBuilder();
    try (var reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
    }
    var body = sb.toString();
    var actualResult = JsonHelper.fromJson(body, ResultDTO.class);
    assertEquals(expectedResult, actualResult);
  }

  @Test
  void testSolveHttpRequestDataSource() throws Exception {
    var uri = new URI("http://" + MDDS_SERVER_HOST + ":" + MDDS_SERVER_PORT + "/solve");
    var url = uri.toURL();

    String boundary = "----TestBoundary";
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setDoOutput(true);
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

    try (var output = connection.getOutputStream()) {
      var writer = new PrintWriter(new OutputStreamWriter(output, UTF_8), true);

      // add data source type
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"dataSourceType\"\r\n\r\n");
      writer.append("http_request").append("\r\n");
      writer.flush();

      // add slaeSolvingMethod
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"slaeSolvingMethod\"\r\n\r\n");
      writer.append("numpy_exact_solver").append("\r\n");
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
    assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    assertEquals("application/json;charset=ISO-8859-1", connection.getContentType());

    try (var reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
      var responseBody = reader.lines().reduce("", (a, b) -> a + b);
      assertTrue(responseBody.contains("id"));
    }
  }

  @Test
  void testSolveMysqlDataSource() throws Exception {
    var uri = new URI("http://" + MDDS_SERVER_HOST + ":" + MDDS_SERVER_PORT + "/solve");
    var url = uri.toURL();

    String boundary = "----TestBoundary";
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setDoOutput(true);
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

    try (var output = connection.getOutputStream()) {
      var writer = new PrintWriter(new OutputStreamWriter(output, UTF_8), true);

      // add slaeSolvingMethod
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"slaeSolvingMethod\"\r\n\r\n");
      writer.append("numpy_exact_solver").append("\r\n");
      writer.flush();

      // add dataSourceType
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"dataSourceType\"\r\n\r\n");
      writer.append("mysql").append("\r\n");
      writer.flush();

      // add mysqlUrl
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"mysqlUrl\"\r\n\r\n");
      writer.append(MY_SQL_CONTAINER.getJdbcUrl()).append("\r\n");
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
    assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    assertEquals("application/json;charset=ISO-8859-1", connection.getContentType());

    try (var reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
      var responseBody = reader.lines().reduce("", (a, b) -> a + b);
      assertTrue(responseBody.contains("id"));
    }
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

  @Test
  void testSolveS3DataSource() throws Exception {
    var uri = new URI("http://" + MDDS_SERVER_HOST + ":" + MDDS_SERVER_PORT + "/solve");
    var url = uri.toURL();

    String boundary = "----TestBoundary";
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setDoOutput(true);
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

    try (var output = connection.getOutputStream()) {
      var writer = new PrintWriter(new OutputStreamWriter(output, UTF_8), true);

      // add slaeSolvingMethod
      writer.append("--").append(boundary).append("\r\n");
      writer.append("Content-Disposition: form-data; name=\"slaeSolvingMethod\"\r\n\r\n");
      writer.append("numpy_exact_solver").append("\r\n");
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
      writer.append("http://" + S_3_HOST + ":" + s3Port).append("\r\n");
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
    assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    assertEquals("application/json;charset=ISO-8859-1", connection.getContentType());

    try (var reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
      var responseBody = reader.lines().reduce("", (a, b) -> a + b);
      assertTrue(responseBody.contains("id"));
    }
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

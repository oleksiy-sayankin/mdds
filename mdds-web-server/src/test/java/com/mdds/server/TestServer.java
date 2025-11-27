/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.common.util.CommonHelper.findFreePort;
import static io.restassured.RestAssured.given;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.mdds.common.util.JsonHelper;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.TaskStatus;
import com.mdds.storage.DataStorageFactory;
import com.mdds.storage.redis.RedisConfFactory;
import com.rabbitmq.client.ConnectionFactory;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
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
  private static final String HOST = ServerConfFactory.fromEnvOrDefaultProperties().host();
  private static final int PORT = findFreePort();
  private static final String WEBAPP =
      ServerConfFactory.fromEnvOrDefaultProperties().webappDirLocation();
  private static final int REDIS_PORT = findFreePort();
  private static RedisServer redisServer;
  private static final String DB_NAME = "testdb";
  private static final String USER_NAME = "testuser";
  private static final String PASSWORD = "testpass";

  private static int s3Port;
  private static final String BUCKET = "test-bucket";
  private static final Region REGION = Region.US_EAST_1;
  private static final String ACCESS = "dummy_access_key_id";
  private static final String SECRET = "dummy_secret_key";

  @Container
  private static final RabbitMQContainer rabbitMq =
      new RabbitMQContainer("rabbitmq:3.12-management")
          .withRabbitMQConfig(MountableFile.forClasspathResource("rabbitmq.conf"))
          .withExposedPorts(5672, 15672);

  @Container
  private static final MySQLContainer<?> mysql =
      new MySQLContainer<>("mysql:8.4.6")
          .withDatabaseName(DB_NAME)
          .withUsername(USER_NAME)
          .withPassword(PASSWORD);

  @Container private static final S3MockContainer s3mock = new S3MockContainer("latest");

  @BeforeAll
  static void startServer() throws LifecycleException, IOException, SQLException {
    redisServer = new RedisServer(REDIS_PORT);
    redisServer.start();

    // Wait for RabbitMq is ready
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(TestServer::rabbitReady);

    System.setProperty("redis.host", "localhost");
    System.setProperty("redis.port", String.valueOf(REDIS_PORT));
    System.setProperty("rabbitmq.host", rabbitMq.getHost());
    System.setProperty("rabbitmq.port", String.valueOf(rabbitMq.getAmqpPort()));
    System.setProperty("rabbitmq.user", rabbitMq.getAdminUsername());
    System.setProperty("rabbitmq.password", rabbitMq.getAdminPassword());

    createMySqlTestData();
    s3Port = s3mock.getMappedPort(9090);
    createS3TestData();

    tomcat = Server.start(HOST, PORT, WEBAPP);

    RestAssured.baseURI = "http://" + HOST + ":" + PORT;
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
  void testRootReturnsIndexHtml() {
    var response =
        given()
            .when()
            .get("/")
            .then()
            .statusCode(200)
            .contentType("text/html")
            .extract()
            .asString();
    assertThat(response).contains("<html");
  }

  @Test
  void testHealthReturnsStatusOk() {
    given().when().get("/health").then().statusCode(200);
  }

  @Test
  void testResultReturnsDataFromDataStorage() {
    var taskId = "test_task_id";
    // Create Result and put it to storage manually
    var expected = new ResultDTO();
    expected.setTaskId(taskId);
    expected.setDateTimeTaskCreated(Instant.now());
    expected.setDateTimeTaskFinished(Instant.now());
    expected.setTaskStatus(TaskStatus.DONE);
    expected.setProgress(100);
    expected.setSolution(new double[] {81.1, 82.2, 37.3, 45.497});

    // We expect Redis service is up and running here
    try (var storage =
        DataStorageFactory.createRedis(RedisConfFactory.fromEnvOrDefaultProperties())) {
      storage.put(taskId, expected);
      // Test that data is in data storage
      var actual = storage.get(taskId, ResultDTO.class);
      Assertions.assertEquals(expected, actual.isPresent() ? actual.get() : actual);
    }

    // Request result using endpoint
    var response =
        given()
            .when()
            .get("/result/" + taskId)
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .extract()
            .asString();

    var actual = JsonHelper.fromJson(response, ResultDTO.class);
    assertEquals(expected, actual);
  }

  @Test
  void testSolveHttpRequestDataSource() {
    var json =
        given()
            .multiPart("dataSourceType", "http_request")
            .multiPart("slaeSolvingMethod", "numpy_exact_solver")
            .multiPart(
                "matrix",
                "matrix.csv",
                "1.3,2.4,3.1\n4.77,5.2321,6.32\n7.23,8.43,9.4343\n".getBytes())
            .multiPart("rhs", "rhs.csv", "1.3\n2.2\n3.7\n".getBytes())
            .when()
            .post("/solve")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .extract()
            .asString();
    var id = JsonHelper.fromJson(json, Map.class).get("id");
    assertThat(id).isNotNull();
  }

  @Test
  void testSolveMysqlDataSource() {
    var json =
        given()
            .multiPart("slaeSolvingMethod", "numpy_exact_solver")
            .multiPart("dataSourceType", "mysql")
            .multiPart("mysqlUrl", mysql.getJdbcUrl())
            .multiPart("mysqlUser", USER_NAME)
            .multiPart("mysqlPassword", PASSWORD)
            .multiPart("mysqlDbName", DB_NAME)
            .multiPart("mysqlMatrixTableName", "MATRIX_TABLE")
            .multiPart("mysqlMatrixJsonFieldName", "JSON_FIELD")
            .multiPart("mysqlMatrixPrimaryKeyFieldName", "ID")
            .multiPart("mysqlMatrixPrimaryKeyFieldValue", "1")
            .multiPart("mysqlRhsTableName", "RHS_TABLE")
            .multiPart("mysqlRhsJsonFieldName", "JSON_FIELD")
            .multiPart("mysqlRhsPrimaryKeyFieldName", "ID")
            .multiPart("mysqlRhsPrimaryKeyFieldValue", "1")
            .when()
            .post("/solve")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .extract()
            .asString();
    var id = JsonHelper.fromJson(json, Map.class).get("id");
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
  void testSolveS3DataSource() {
    var endpoint = "http://localhost:" + s3Port;
    var json =
        given()
            .multiPart("slaeSolvingMethod", "numpy_exact_solver")
            .multiPart("dataSourceType", "s3")
            .multiPart("awsBucketName", BUCKET)
            .multiPart("awsUseEndPointUrl", "true")
            .multiPart("awsEndPointUrl", endpoint)
            .multiPart("awsRegion", REGION.id())
            .multiPart("awsAccessKeyId", ACCESS)
            .multiPart("awsSecretAccessKey", SECRET)
            .multiPart("awsMatrixKey", "matrix.json")
            .multiPart("awsRhsKey", "rhs.json")
            .multiPart("awsPathStyleAccessEnabled", "true")
            .when()
            .post("/solve")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .extract()
            .asString();
    var id = JsonHelper.fromJson(json, Map.class).get("id");
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

  private static boolean rabbitReady() throws IOException, TimeoutException {
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

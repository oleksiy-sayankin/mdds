/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.data.source.DataSourceDescriptor.Type.HTTP_REQUEST;
import static com.mdds.data.source.DataSourceDescriptor.Type.MYSQL;
import static com.mdds.data.source.DataSourceDescriptor.Type.S3;
import static com.mdds.data.source.DataSourceProviderFactory.fromDescriptor;
import static com.mdds.server.ServerHelper.extractDescriptor;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
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
class TestServerHelper {
  private static final String DB_NAME = "testdb";
  private static final String USER_NAME = "testuser";
  private static final String PASSWORD = "testpass";

  @Container
  private static final MySQLContainer<?> MY_SQL_CONTAINER =
      new MySQLContainer<>("mysql:8.4.6")
          .withDatabaseName(DB_NAME)
          .withUsername(USER_NAME)
          .withPassword(PASSWORD);

  @Container
  private static final S3MockContainer S_3_MOCK_CONTAINER = new S3MockContainer("latest");

  private static final String S_3_HOST = "localhost";
  private static int s3Port;
  private static final String S_3_BUCKET = "test-bucket";
  private static final Region S_3_REGION = Region.US_EAST_1;
  private static final String S_3_ACCESS_KEY_ID = "dummy_access_key_id";
  private static final String S_3_SECRET_ACCESS_KEY = "dummy_secret_key";
  private static final String MATRIX_KEY = "matrix.json";
  private static final String RHS_KEY = "rhs.json";
  private static final boolean PATH_STYLE_ACCESS_ENABLED = true;

  @BeforeAll
  static void init() throws SQLException {
    createMySqlTestData();
    s3Port = S_3_MOCK_CONTAINER.getMappedPort(9090);
    createS3TestData();
  }

  @Test
  void testExtractDescriptorHttpRequestDataSource() {
    var actualDescriptor =
        extractDescriptor(
            "http_request",
            Map.of(
                "matrix",
                List.of(List.of(1.3, 2.2, 3.7), List.of(7.7, 2.1, 9.3), List.of(1.1, 4.8, 2.3)),
                "rhs",
                List.of(1.3, 2.2, 3.7)));
    assertThat(actualDescriptor.isPresent()).isTrue();
    var descriptor = actualDescriptor.get();
    assertThat(descriptor.getType()).isEqualTo(HTTP_REQUEST);
    var actualProvider = fromDescriptor(descriptor);
    assertThat(actualProvider.isPresent()).isTrue();
    var provider = actualProvider.get();
    var actualMatrix = provider.loadMatrix();
    assertThat(actualMatrix.isPresent()).isTrue();
    var matrix = actualMatrix.get();
    assertThat(matrix)
        .isEqualTo(new double[][] {{1.3, 2.2, 3.7}, {7.7, 2.1, 9.3}, {1.1, 4.8, 2.3}});
    var actualRhs = provider.loadRhs();
    assertThat(actualRhs.isPresent()).isTrue();
    var rhs = actualRhs.get();
    assertThat(rhs).isEqualTo(new double[] {1.3, 2.2, 3.7});
  }

  @Test
  void testExtractDescriptorMySqlDataSource() {
    var actualDescriptor =
        extractDescriptor(
            "mysql",
            Map.ofEntries(
                Map.entry("mysql.url", MY_SQL_CONTAINER.getJdbcUrl()),
                Map.entry("mysql.user", USER_NAME),
                Map.entry("mysql.password", PASSWORD),
                Map.entry("mysql.db.name", DB_NAME),
                Map.entry("mysql.matrix.table.name", "MATRIX_TABLE"),
                Map.entry("mysql.matrix.json.field.name", "JSON_FIELD"),
                Map.entry("mysql.matrix.primary.key.field.name", "ID"),
                Map.entry("mysql.matrix.primary.key.field.value", "1"),
                Map.entry("mysql.rhs.table.name", "RHS_TABLE"),
                Map.entry("mysql.rhs.json.field.name", "JSON_FIELD"),
                Map.entry("mysql.rhs.primary.key.field.name", "ID"),
                Map.entry("mysql.rhs.primary.key.field.value", "1")));
    assertThat(actualDescriptor.isPresent()).isTrue();
    var descriptor = actualDescriptor.get();
    assertThat(descriptor.getType()).isEqualTo(MYSQL);
    var actualProvider = fromDescriptor(descriptor);
    assertThat(actualProvider.isPresent()).isTrue();
    var provider = actualProvider.get();
    var actualMatrix = provider.loadMatrix();
    assertThat(actualMatrix.isPresent()).isTrue();
    var matrix = actualMatrix.get();
    assertThat(matrix)
        .isEqualTo(new double[][] {{3.4, 5.5, 2.2}, {1.2, 5.5, 8.1}, {3.4, 8.6, 9.4}});
    var actualRhs = provider.loadRhs();
    assertThat(actualRhs.isPresent()).isTrue();
    var rhs = actualRhs.get();
    assertThat(rhs).isEqualTo(new double[] {4.7, 1.5, 5.3});
  }

  @Test
  void testExtractDescriptorS3DataSource() {
    var actualDescriptor =
        extractDescriptor(
            "s3",
            Map.of(
                "aws.bucket.name", S_3_BUCKET,
                "aws.use.endpoint.url", "true",
                "aws.endpoint.url", "http://" + S_3_HOST + ":" + s3Port,
                "aws.region", S_3_REGION.id(),
                "aws.access.key.id", S_3_ACCESS_KEY_ID,
                "aws.secret.access.key", S_3_SECRET_ACCESS_KEY,
                "aws.matrix.key", MATRIX_KEY,
                "aws.rhs.key", RHS_KEY,
                "aws.path.style.access.enabled", "true"));
    assertThat(actualDescriptor.isPresent()).isTrue();
    var descriptor = actualDescriptor.get();
    assertThat(descriptor.getType()).isEqualTo(S3);
    var actualProvider = fromDescriptor(descriptor);
    assertThat(actualProvider.isPresent()).isTrue();
    var provider = actualProvider.get();
    var actualMatrix = provider.loadMatrix();
    assertThat(actualMatrix.isPresent()).isTrue();
    var matrix = actualMatrix.get();
    assertThat(matrix)
        .isEqualTo(new double[][] {{1.2, 3.343, 543}, {4.32, 243.3, 2.232}, {7.32, 32.32, 432.1}});
    var actualRhs = provider.loadRhs();
    assertThat(actualRhs.isPresent()).isTrue();
    var rhs = actualRhs.get();
    assertThat(rhs).isEqualTo(new double[] {4.3, 6.2, 3.3});
  }

  @Test
  void testExtractDescriptorNoData() {
    var actualDescriptor =
        extractDescriptor(
            null,
            Map.of(
                "matrix",
                List.of(List.of(1.3, 2.2, 3.7), List.of(7.7, 2.1, 9.3), List.of(1.1, 4.8, 2.3)),
                "rhs",
                List.of(1.3, 2.2, 3.7)));
    assertThat(actualDescriptor.isFailure()).isTrue();
    assertThat(actualDescriptor.getErrorMessage()).isEqualTo("Invalid data source type: null");
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
}

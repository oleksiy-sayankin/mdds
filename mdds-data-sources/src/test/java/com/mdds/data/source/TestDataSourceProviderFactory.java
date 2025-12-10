/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
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
class TestDataSourceProviderFactory {
  private static String jdbcUrl;
  private static final String DB_NAME = "testdb";
  private static final String USER_NAME = "testuser";
  private static final String PASSWORD = "testpass";

  @Container
  private static final MySQLContainer<?> MY_SQL_CONTAINER =
      new MySQLContainer<>("mysql:8.4.6")
          .withDatabaseName(DB_NAME)
          .withUsername(USER_NAME)
          .withPassword(PASSWORD);

  @Container private static final S3MockContainer s3MockContainer = new S3MockContainer("latest");
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
  static void setUp() throws SQLException {
    jdbcUrl = MY_SQL_CONTAINER.getJdbcUrl();
    createMySqlTestData();

    s3Port = s3MockContainer.getMappedPort(9090);
    var endpoint = s3MockContainer.getHttpEndpoint();
    var serviceConfig =
        S3Configuration.builder().pathStyleAccessEnabled(PATH_STYLE_ACCESS_ENABLED).build();
    var httpClient =
        UrlConnectionHttpClient.builder()
            .buildWithDefaults(
                AttributeMap.builder().put(TRUST_ALL_CERTIFICATES, Boolean.TRUE).build());

    S3Client s3Client =
        S3Client.builder()
            .endpointOverride(URI.create(endpoint))
            .serviceConfiguration(serviceConfig)
            .httpClient(httpClient)
            .region(S_3_REGION)
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(S_3_ACCESS_KEY_ID, S_3_SECRET_ACCESS_KEY)))
            .build();
    createS3TestData(s3Client);
  }

  @Test
  void testHttpRequestFromDescriptor() {
    var matrix = new double[][] {{1.3, 2.2, 3.7}, {7.7, 2.1, 9.3}, {1.1, 4.8, 2.3}};
    var rhs = new double[] {1.3, 2.2, 3.7};
    Map<String, Object> params = new HashMap<>();
    params.put(
        "matrix",
        List.of(
            List.of(matrix[0][0], matrix[0][1], matrix[0][2]),
            List.of(matrix[1][0], matrix[1][1], matrix[1][2]),
            List.of(matrix[2][0], matrix[2][1], matrix[2][2])));
    params.put("rhs", List.of(rhs[0], rhs[1], rhs[2]));
    var dsd = DataSourceDescriptor.of(DataSourceDescriptor.Type.HTTP_REQUEST, params);
    var dataSourceProvider = DataSourceProviderFactory.fromDescriptor(dsd);
    dataSourceProvider.ifPresent(
        dsp -> {
          dsp.loadMatrix().ifPresent(actualMatrix -> assertThat(matrix).isEqualTo(actualMatrix));
          dsp.loadRhs().ifPresent(actualRhs -> assertThat(rhs).isEqualTo(actualRhs));
        });
  }

  @Test
  void testMySqlFromDescriptor() {
    Map<String, Object> params = new HashMap<>();
    params.put("mysql.url", jdbcUrl);
    params.put("mysql.user", USER_NAME);
    params.put("mysql.password", PASSWORD);
    params.put("mysql.db.name", DB_NAME);
    params.put("mysql.rhs.table.name", "rhs_table");
    params.put("mysql.rhs.json.field.name", "json_field");
    params.put("mysql.rhs.primary.key.field.name", "id");
    params.put("mysql.rhs.primary.key.field.value", "1984");
    var expectedMatrix = new double[][] {{3.4, 5.5, 2.2}, {1.2, 5.5, 8.1}, {3.4, 8.6, 9.4}};
    var expectedRhs = new double[] {4.7, 1.5, 5.3};
    var dsd = DataSourceDescriptor.of(DataSourceDescriptor.Type.MYSQL, params);
    var dataSourceProvider = DataSourceProviderFactory.fromDescriptor(dsd);
    dataSourceProvider.ifPresent(
        dsp -> {
          dsp.loadMatrix()
              .ifPresent(actualMatrix -> assertThat(expectedMatrix).isEqualTo(actualMatrix));
          dsp.loadRhs().ifPresent(actualRhs -> assertThat(expectedRhs).isEqualTo(actualRhs));
        });
  }

  @Test
  void testS3FromDescriptor() {
    var expectedMatrix =
        new double[][] {{1.2, 3.343, 543}, {4.32, 243.3, 2.232}, {7.32, 32.32, 432.1}};
    var expectedRhs = new double[] {4.3, 6.2, 3.3};
    var params = new HashMap<String, Object>();
    params.put("aws.bucket.name", S_3_BUCKET);
    params.put("aws.use.endpoint.url", "true");
    params.put("aws.endpoint.url", "http://" + S_3_HOST + ":" + s3Port);
    params.put("aws.region", S_3_REGION.id());
    params.put("aws.access.key.id", S_3_ACCESS_KEY_ID);
    params.put("aws.secret.access.key", S_3_SECRET_ACCESS_KEY);
    params.put("aws.matrix.key", MATRIX_KEY);
    params.put("aws.rhs.key", RHS_KEY);
    params.put("aws.path.style.access.enabled", String.valueOf(PATH_STYLE_ACCESS_ENABLED));
    var dsd = DataSourceDescriptor.of(DataSourceDescriptor.Type.S3, params);
    var dataSourceProvider = DataSourceProviderFactory.fromDescriptor(dsd);
    dataSourceProvider.ifPresent(
        dsp -> {
          dsp.loadMatrix()
              .ifPresent(actualMatrix -> assertThat(expectedMatrix).isEqualTo(actualMatrix));
          dsp.loadRhs().ifPresent(actualRhs -> assertThat(expectedRhs).isEqualTo(actualRhs));
        });
  }

  private static void createMySqlTestData() throws SQLException {
    try (var connection = DriverManager.getConnection(jdbcUrl, USER_NAME, PASSWORD);
        var stmt = connection.createStatement(); ) {
      stmt.execute("USE " + DB_NAME);
      stmt.execute("CREATE TABLE MATRIX_TABLE (ID INT PRIMARY KEY, JSON_FIELD VARCHAR(255))");
      stmt.execute("CREATE TABLE RHS_TABLE (ID INT PRIMARY KEY, JSON_FIELD VARCHAR(255))");
      stmt.execute(
          "INSERT INTO MATRIX_TABLE (ID, JSON_FIELD) VALUES (1984,"
              + " '[[3.4,5.5,2.2],[1.2,5.5,8.1],[3.4,8.6,9.4]]')");
      stmt.execute("INSERT INTO RHS_TABLE (ID, JSON_FIELD) VALUES (1984," + " '[4.7,1.5,5.3]')");
    }
  }

  private static void createS3TestData(S3Client s3Client) {
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

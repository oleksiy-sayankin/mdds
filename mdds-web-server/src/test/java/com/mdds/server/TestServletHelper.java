/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.github.valfirst.slf4jtest.TestLoggerFactory.getTestLogger;
import static com.mdds.data.source.DataSourceDescriptor.Type.*;
import static com.mdds.data.source.DataSourceProviderFactory.fromDescriptor;
import static com.mdds.server.ServletHelper.*;
import static jakarta.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.mdds.dto.SlaeSolver;
import com.mdds.queue.Queue;
import com.mdds.storage.DataStorage;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.Part;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
class TestServletHelper {
  private HttpServletRequest request;
  private HttpServletResponse response;
  private ServletContext servletContext;
  private Queue queue;
  private DataStorage dataStorage;
  private ServerService serverService;
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

  @BeforeEach
  void setUp() {
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
    servletContext = mock(ServletContext.class);
    queue = mock(Queue.class);
    dataStorage = mock(DataStorage.class);
    serverService = mock(ServerService.class);
  }

  @Test
  void testExtractQueueWhenQueueExists() {
    when(servletContext.getAttribute(ServerAppContextListener.ATTR_SERVER_SERVICE))
        .thenReturn(serverService);
    when(serverService.getQueue()).thenReturn(queue);
    var actualQueue = extractQueue(servletContext);
    actualQueue.ifPresent(q -> assertEquals(queue, q));
  }

  @Test
  void testExtractQueueWhenNoQueue() {
    var actualQueue = extractQueue(servletContext);
    assertTrue(actualQueue.isFailure());
    assertEquals(
        "Servlet context attribute SERVER_SERVICE is null.", actualQueue.getErrorMessage());
  }

  @Test
  void testExtractDataStorageWhenDataStorageExists() {
    when(servletContext.getAttribute(ServerAppContextListener.ATTR_SERVER_SERVICE))
        .thenReturn(serverService);
    when(serverService.getDataStorage()).thenReturn(dataStorage);
    var actualDataStorage = extractDataStorage(servletContext);
    actualDataStorage.ifPresent(ds -> assertEquals(dataStorage, ds));
  }

  @Test
  void testExtractSolvingMethodWhenMethodExists() {
    when(request.getParameter("slaeSolvingMethod")).thenReturn("numpy_exact_solver");
    var actualSolvingMethod = extractSolvingMethod(request);
    actualSolvingMethod.ifPresent(asm -> assertEquals(SlaeSolver.NUMPY_EXACT_SOLVER, asm));
  }

  @Test
  void testExtractSolvingMethodWhenWrongMethod() {
    var solvingMethod = "wrong_solving_method";
    when(request.getParameter("slaeSolvingMethod")).thenReturn(solvingMethod);
    var actualSolvingMethod = extractSolvingMethod(request);
    assertTrue(actualSolvingMethod.isFailure());
    assertEquals(
        "Invalid solving method: wrong_solving_method", actualSolvingMethod.getErrorMessage());
  }

  @Test
  void testExtractSolvingMethodWhenNullMethod() {
    when(request.getParameter("slaeSolvingMethod")).thenReturn(null);
    var actualSolvingMethod = extractSolvingMethod(request);
    assertTrue(actualSolvingMethod.isFailure());
    assertEquals("Invalid solving method: null", actualSolvingMethod.getErrorMessage());
  }

  @Test
  void testSendError() throws IOException {
    var message = "Test error message";
    doNothing().when(response).sendError(anyInt(), anyString());
    sendError(response, SC_BAD_REQUEST, message);
    verify(response).sendError(SC_BAD_REQUEST, message);
  }

  @Test
  void testSendErrorAndLogException() throws IOException {
    var logger = getTestLogger(ServletHelper.class);
    var message = "Test error message";
    doThrow(new IOException()).when(response).sendError(anyInt(), anyString());
    sendError(response, SC_BAD_REQUEST, message);
    verify(response).sendError(SC_BAD_REQUEST, message);
    assertTrue(logger.getLoggingEvents().stream().anyMatch(e -> e.getMessage().contains(message)));
  }

  @Test
  void testExtractDescriptorHttpRequestDataSource() throws ServletException, IOException {
    when(request.getParameter("dataSourceType")).thenReturn("http_request");
    var expectedMatrixAsString =
        new String[][] {{"1.3", "2.2", "3.7"}, {"7.7", "2.1", "9.3"}, {"1.1", "4.8", "2.3"}};
    var sb = new StringBuilder();
    for (var row : expectedMatrixAsString) {
      sb.append(String.join(",", row));
      sb.append(System.lineSeparator());
    }
    var matrixIs = new ByteArrayInputStream(sb.toString().getBytes(UTF_8));
    var matrixPart = mock(Part.class);
    when(matrixPart.getInputStream()).thenReturn(matrixIs);
    when(request.getPart("matrix")).thenReturn(matrixPart);

    var expectedRhsAsString = new String[] {"1.3", "2.2", "3.7"};
    var rhsIs =
        new ByteArrayInputStream(
            String.join(System.lineSeparator(), expectedRhsAsString).getBytes(UTF_8));
    var rhsPart = mock(Part.class);
    when(rhsPart.getInputStream()).thenReturn(rhsIs);
    when(request.getPart("rhs")).thenReturn(rhsPart);

    var actualDescriptor = extractDescriptor(request);
    assertTrue(actualDescriptor.isPresent());
    actualDescriptor.ifPresent(
        descriptor -> {
          assertEquals(HTTP_REQUEST, descriptor.getType());
          var actualProvider = fromDescriptor(descriptor);
          assertTrue(actualProvider.isPresent());
          actualProvider.ifPresent(
              provider -> {
                var actualMatrix = provider.loadMatrix();
                assertTrue(actualMatrix.isPresent());
                actualMatrix.ifPresent(
                    matrix -> {
                      var expectedMatrix =
                          new double[][] {{1.3, 2.2, 3.7}, {7.7, 2.1, 9.3}, {1.1, 4.8, 2.3}};
                      assertArrayEquals(expectedMatrix, matrix);
                    });
                var actualRhs = provider.loadRhs();
                assertTrue(actualRhs.isPresent());
                actualRhs.ifPresent(
                    rhs -> {
                      var expectedRhs = new double[] {1.3, 2.2, 3.7};
                      assertArrayEquals(expectedRhs, rhs);
                    });
              });
        });
  }

  @Test
  void testExtractDescriptorMySqlDataSource() {
    when(request.getParameter("dataSourceType")).thenReturn("mysql");
    when(request.getParameter("slaeSolvingMethod")).thenReturn("numpy_exact_solver");
    when(request.getParameter("mysqlUrl")).thenReturn(MY_SQL_CONTAINER.getJdbcUrl());
    when(request.getParameter("mysqlUser")).thenReturn(USER_NAME);
    when(request.getParameter("mysqlPassword")).thenReturn(PASSWORD);
    when(request.getParameter("mysqlDbName")).thenReturn(DB_NAME);
    when(request.getParameter("mysqlMatrixTableName")).thenReturn("MATRIX_TABLE");
    when(request.getParameter("mysqlMatrixJsonFieldName")).thenReturn("JSON_FIELD");
    when(request.getParameter("mysqlMatrixPrimaryKeyFieldName")).thenReturn("ID");
    when(request.getParameter("mysqlMatrixPrimaryKeyFieldValue")).thenReturn("1");
    when(request.getParameter("mysqlRhsTableName")).thenReturn("RHS_TABLE");
    when(request.getParameter("mysqlRhsJsonFieldName")).thenReturn("JSON_FIELD");
    when(request.getParameter("mysqlRhsPrimaryKeyFieldName")).thenReturn("ID");
    when(request.getParameter("mysqlRhsPrimaryKeyFieldValue")).thenReturn("1");
    var actualDescriptor = extractDescriptor(request);
    assertTrue(actualDescriptor.isPresent());

    actualDescriptor.ifPresent(
        descriptor -> {
          assertEquals(MYSQL, descriptor.getType());
          var actualProvider = fromDescriptor(descriptor);
          assertTrue(actualProvider.isPresent());
          actualProvider.ifPresent(
              provider -> {
                var actualMatrix = provider.loadMatrix();
                assertTrue(actualMatrix.isPresent());
                actualMatrix.ifPresent(
                    matrix -> {
                      var expectedMatrix =
                          new double[][] {{3.4, 5.5, 2.2}, {1.2, 5.5, 8.1}, {3.4, 8.6, 9.4}};
                      assertArrayEquals(expectedMatrix, matrix);
                    });
                var actualRhs = provider.loadRhs();
                assertTrue(actualRhs.isPresent());
                actualRhs.ifPresent(
                    rhs -> {
                      var expectedRhs = new double[] {4.7, 1.5, 5.3};
                      assertArrayEquals(expectedRhs, rhs);
                    });
              });
        });
  }

  @Test
  void testExtractDescriptorS3DataSource() {
    when(request.getParameter("dataSourceType")).thenReturn("s3");
    when(request.getParameter("awsBucketName")).thenReturn(S_3_BUCKET);
    when(request.getParameter("awsUseEndPointUrl")).thenReturn("true");
    when(request.getParameter("awsEndPointUrl")).thenReturn("http://" + S_3_HOST + ":" + s3Port);
    when(request.getParameter("awsRegion")).thenReturn(S_3_REGION.id());
    when(request.getParameter("awsAccessKeyId")).thenReturn(S_3_ACCESS_KEY_ID);
    when(request.getParameter("awsSecretAccessKey")).thenReturn(S_3_SECRET_ACCESS_KEY);
    when(request.getParameter("awsMatrixKey")).thenReturn(MATRIX_KEY);
    when(request.getParameter("awsRhsKey")).thenReturn(RHS_KEY);
    when(request.getParameter("awsPathStyleAccessEnabled")).thenReturn("true");
    var actualDescriptor = extractDescriptor(request);
    assertTrue(actualDescriptor.isPresent());
    actualDescriptor.ifPresent(
        descriptor -> {
          assertEquals(S3, descriptor.getType());
          var actualProvider = fromDescriptor(descriptor);
          assertTrue(actualProvider.isPresent());
          actualProvider.ifPresent(
              provider -> {
                var actualMatrix = provider.loadMatrix();
                assertTrue(actualMatrix.isPresent());
                actualMatrix.ifPresent(
                    matrix -> {
                      var expectedMatrix =
                          new double[][] {
                            {1.2, 3.343, 543}, {4.32, 243.3, 2.232}, {7.32, 32.32, 432.1}
                          };
                      assertArrayEquals(expectedMatrix, matrix);
                    });

                var actualRhs = provider.loadRhs();
                assertTrue(actualRhs.isPresent());
                actualRhs.ifPresent(
                    rhs -> {
                      var expectedRhs = new double[] {4.3, 6.2, 3.3};
                      assertArrayEquals(expectedRhs, rhs);
                    });
              });
        });
  }

  @Test
  void testExtractDescriptorNoData() {
    when(request.getParameter("dataSourceType")).thenReturn(null);
    var actualDataSourceType = extractDescriptor(request);
    assertTrue(actualDataSourceType.isFailure());
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

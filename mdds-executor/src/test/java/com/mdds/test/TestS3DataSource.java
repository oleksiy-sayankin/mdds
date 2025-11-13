/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.mdds.common.util.JsonHelper;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.SlaeSolver;
import com.mdds.dto.TaskIdResponseDTO;
import com.mdds.dto.TaskStatus;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
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

class TestS3DataSource extends BaseEnvironment {
  private static final String S_3_HOST = "s3host";
  private static final int S_3_PORT = 9090;
  private static final String S_3_BUCKET = "test-bucket";
  private static final Region S_3_REGION = Region.US_EAST_1;
  private static final String S_3_ACCESS_KEY_ID = "dummy_access_key_id";
  private static final String S_3_SECRET_ACCESS_KEY = "dummy_secret_key";
  private static final String MATRIX_KEY = "matrix.json";
  private static final String RHS_KEY = "rhs.json";
  private static final boolean PATH_STYLE_ACCESS_ENABLED = true;

  @Container
  private static final S3MockContainer S_3_MOCK_CONTAINER =
      new S3MockContainer("latest").withNetwork(SHARED_NETWORK).withNetworkAliases(S_3_HOST);

  @BeforeAll
  static void setupS3() {
    awaitReady(TestS3DataSource::s3MockIsReady, "S3");
    createS3TestData();
  }

  @ParameterizedTest
  @MethodSource("solvers")
  void testHttpRequest(SlaeSolver slaeSolver) throws Exception {
    var uri =
        new AtomicReference<>(
            new URI("http://" + getMddsWebServerHost() + ":" + getMddsWebServerPort() + "/solve"));
    var url = new AtomicReference<>(uri.get().toURL());
    var connection = new AtomicReference<>((HttpURLConnection) url.get().openConnection());
    connection.get().setDoOutput(true);
    connection.get().setRequestMethod("POST");
    connection
        .get()
        .setRequestProperty("Content-Type", "multipart/form-data; boundary=" + BOUNDARY);

    try (var output = connection.get().getOutputStream()) {
      var writer = new PrintWriter(new OutputStreamWriter(output, UTF_8), true);
      appendTo(writer, "slaeSolvingMethod", slaeSolver.getName());
      appendTo(writer, "dataSourceType", "s3");
      appendTo(writer, "awsBucketName", S_3_BUCKET);
      appendTo(writer, "awsUseEndPointUrl", "true");
      appendTo(writer, "awsEndPointUrl", "http://" + S_3_HOST + ":" + S_3_PORT);
      appendTo(writer, "awsRegion", S_3_REGION.id());
      appendTo(writer, "awsAccessKeyId", S_3_ACCESS_KEY_ID);
      appendTo(writer, "awsSecretAccessKey", S_3_SECRET_ACCESS_KEY);
      appendTo(writer, "awsMatrixKey", MATRIX_KEY);
      appendTo(writer, "awsRhsKey", RHS_KEY);
      appendTo(writer, "awsPathStyleAccessEnabled", "true");

      // finish the request
      writer.append("--").append(BOUNDARY).append("--").append("\r\n");
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
                          + getMddsWebServerHost()
                          + ":"
                          + getMddsWebServerPort()
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
    try (var s3Client = createS3Client()) {
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
    try (var s3Client = createS3Client()) {
      s3Client.listBuckets();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static S3Client createS3Client() {
    var endpoint = S_3_MOCK_CONTAINER.getHttpEndpoint();
    var serviceConfig =
        S3Configuration.builder().pathStyleAccessEnabled(PATH_STYLE_ACCESS_ENABLED).build();
    var httpClient =
        UrlConnectionHttpClient.builder()
            .buildWithDefaults(
                AttributeMap.builder().put(TRUST_ALL_CERTIFICATES, Boolean.TRUE).build());
    return S3Client.builder()
        .endpointOverride(URI.create(endpoint))
        .serviceConfiguration(serviceConfig)
        .httpClient(httpClient)
        .region(S_3_REGION)
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(S_3_ACCESS_KEY_ID, S_3_SECRET_ACCESS_KEY)))
        .build();
  }
}

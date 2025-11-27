/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.test;

import static io.restassured.RestAssured.given;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.mdds.common.util.JsonHelper;
import com.mdds.dto.SlaeSolver;
import com.mdds.dto.TaskIdResponseDTO;
import com.mdds.dto.TaskStatus;
import io.restassured.http.ContentType;
import java.net.URI;
import java.nio.ByteBuffer;
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
  private static final String S3_HOST_ALIAS = "s3host";
  private static final int S3_PORT = 9090;
  private static final String BUCKET = "test-bucket";
  private static final Region REGION = Region.US_EAST_1;
  private static final String ACCESS = "dummy_access_key_id";
  private static final String SECRET = "dummy_secret_key";

  @Container
  private static final S3MockContainer S_3_MOCK_CONTAINER =
      new S3MockContainer("latest").withNetwork(SHARED_NETWORK).withNetworkAliases(S3_HOST_ALIAS);

  @BeforeAll
  static void setupS3() {
    awaitReady(TestS3DataSource::s3MockIsReady, "S3");
    createS3TestData();
  }

  @ParameterizedTest
  @MethodSource("solvers")
  void testHttpRequest(SlaeSolver solver) {
    var json =
        given()
            .multiPart("slaeSolvingMethod", solver.getName())
            .multiPart("dataSourceType", "s3")
            .multiPart("awsBucketName", BUCKET)
            .multiPart("awsUseEndPointUrl", "true")
            .multiPart("awsEndPointUrl", "http://" + S3_HOST_ALIAS + ":" + S3_PORT)
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

    var taskId = JsonHelper.fromJson(json, TaskIdResponseDTO.class).getId();
    assertThat(taskId).as("Task id should not be null").isNotNull();
    var actual = awaitForResult(taskId);

    assertThat(actual.getTaskStatus()).isEqualTo(TaskStatus.DONE);

    double[] expected = {
      -0.1499382089687040253643, 0.0280711223847708241758, 0.0080775029256540625510
    };
    assertDoneAndEquals(expected, actual);
  }

  private static void createS3TestData() {
    try (var s3Client = createS3Client()) {
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
    var serviceConfig = S3Configuration.builder().pathStyleAccessEnabled(true).build();
    var httpClient =
        UrlConnectionHttpClient.builder()
            .buildWithDefaults(
                AttributeMap.builder().put(TRUST_ALL_CERTIFICATES, Boolean.TRUE).build());
    return S3Client.builder()
        .endpointOverride(URI.create(endpoint))
        .serviceConfiguration(serviceConfig)
        .httpClient(httpClient)
        .region(REGION)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS, SECRET)))
        .build();
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.s3;

import static com.mdds.data.source.provider.s3.S3Helper.extractMatrix;
import static com.mdds.data.source.provider.s3.S3Helper.extractRhs;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
class TestS3Helper {
  @Container private static final S3MockContainer s3MockContainer = new S3MockContainer("latest");
  private static S3Client s3Client;
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
  static void init() {
    s3Port = s3MockContainer.getMappedPort(9090);
    var endpoint = s3MockContainer.getHttpEndpoint();
    var serviceConfig =
        S3Configuration.builder().pathStyleAccessEnabled(PATH_STYLE_ACCESS_ENABLED).build();
    var httpClient =
        UrlConnectionHttpClient.builder()
            .buildWithDefaults(
                AttributeMap.builder().put(TRUST_ALL_CERTIFICATES, Boolean.TRUE).build());

    s3Client =
        S3Client.builder()
            .endpointOverride(URI.create(endpoint))
            .serviceConfiguration(serviceConfig)
            .httpClient(httpClient)
            .region(S_3_REGION)
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(S_3_ACCESS_KEY_ID, S_3_SECRET_ACCESS_KEY)))
            .build();
    createTestData(s3Client);
  }

  @AfterAll
  static void close() {
    s3Client.close();
  }

  private static void createTestData(S3Client s3Client) {
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

  @Test
  void testExtractMatrix() {
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
    var s3Config = S3Config.of(params);
    var actualMatrix = extractMatrix(s3Config);
    var expectedMatrix =
        new double[][] {{1.2, 3.343, 543}, {4.32, 243.3, 2.232}, {7.32, 32.32, 432.1}};
    assertThat(actualMatrix.get()).isEqualTo(expectedMatrix);
  }

  @Test
  void testExtractRhs() {
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
    var s3Config = S3Config.of(params);
    var actualRhs = extractRhs(s3Config);
    var expectedRhs = new double[] {4.3, 6.2, 3.3};
    assertThat(actualRhs.get()).isEqualTo(expectedRhs);
  }
}

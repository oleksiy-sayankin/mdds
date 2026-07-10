/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.server.PresignedUrlAssertions.assertExpiresAtMatchesSignature;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdds.common.util.JsonHelper;
import com.mdds.dto.ManifestDTO;
import io.minio.GetObjectArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.errors.MinioException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
class TestObjectStorageService {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String MINIO_BUCKET = "mdds";

  private static MinioClient minioClient;

  @Autowired private ObjectStorageService objectStorageService;
  @Autowired private ObjectStorageProperties objectStorageProperties;

  @Container
  private static final MinIOContainer MINIO_CONTAINER =
      new MinIOContainer("minio/minio:RELEASE.2022-12-02T19-19-22Z")
          .withUserName("testuser")
          .withPassword("testpassword");

  @Container
  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17")
          .withDatabaseName("mdds")
          .withUsername("mdds")
          .withPassword("mdds123");

  @DynamicPropertySource
  static void initProps(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("mdds.object-storage.access-key", MINIO_CONTAINER::getUserName);
    registry.add("mdds.object-storage.secret-key", MINIO_CONTAINER::getPassword);
    registry.add("mdds.object-storage.public-endpoint", MINIO_CONTAINER::getS3URL);
    registry.add("mdds.object-storage.internal-endpoint", MINIO_CONTAINER::getS3URL);
    registry.add("mdds.object-storage.bucket", () -> MINIO_BUCKET);
    registry.add("mdds.object-storage.region", () -> "us-east-1");
    registry.add("mdds.object-storage.path-style-access-enabled", () -> "true");
  }

  @BeforeAll
  static void initMinio() throws MinioException {
    minioClient =
        MinioClient.builder()
            .endpoint(MINIO_CONTAINER.getS3URL())
            .credentials(MINIO_CONTAINER.getUserName(), MINIO_CONTAINER.getPassword())
            .build();

    minioClient.makeBucket(MakeBucketArgs.builder().bucket(MINIO_BUCKET).build());
  }

  @Test
  void testIssueUploadUrl() throws URISyntaxException {
    var bucket = objectStorageProperties.bucket();
    var userId = 42L;
    var jobId = UUID.randomUUID().toString();
    var fileName = "matrix.csv";
    var result = objectStorageService.issueUploadUrl(userId, jobId, fileName);
    var url = result.uploadUrl();
    assertThat(url).isNotNull();
    assertExpiresAtMatchesSignature(
        result.expiresAt(), result.uploadUrl(), objectStorageProperties.presignPutTtl());
    assertThat(url.getPath())
        .isEqualTo("/" + bucket + "/jobs/" + userId + "/" + jobId + "/in/" + fileName);
  }

  @Test
  void testPutManifestUploadsManifestToMinioWithChecksumCompatibleS3Client() throws Exception {
    var userId = 42L;
    var jobId = "job-" + UUID.randomUUID();
    var manifestObjectKey = "jobs/" + userId + "/" + jobId + "/manifest.json";

    var manifestJson =
        """
        {
          "manifestVersion": 1,
          "userId": %d,
          "jobId": "%s",
          "jobType": "solving_slae",
          "inputs": {
            "matrix": {
              "objectKey": "jobs/%d/%s/in/matrix.csv",
              "format": "csv"
            },
            "rhs": {
              "objectKey": "jobs/%d/%s/in/rhs.csv",
              "format": "csv"
            }
          },
          "params": {
            "solvingMethod": "numpy_exact_solver"
          },
          "outputs": {
            "solution": {
              "objectKey": "jobs/%d/%s/out/solution.csv",
              "format": "csv"
            }
          }
        }
        """
            .formatted(userId, jobId, userId, jobId, userId, jobId, userId, jobId);

    var manifest = JsonHelper.fromJson(manifestJson, ManifestDTO.class);
    objectStorageService.putManifest(manifestObjectKey, manifest);
    var uploadedManifestJson = readObject(manifestObjectKey);
    assertThat(uploadedManifestJson).isNotBlank();
    assertThat(MAPPER.readTree(uploadedManifestJson)).isEqualTo(MAPPER.readTree(manifestJson));
  }

  private static String readObject(String key) throws MinioException {
    try (var stream =
        minioClient.getObject(GetObjectArgs.builder().bucket(MINIO_BUCKET).object(key).build())) {
      return new BufferedReader(new InputStreamReader(stream, UTF_8))
          .lines()
          .collect(Collectors.joining("\n"));
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }
}

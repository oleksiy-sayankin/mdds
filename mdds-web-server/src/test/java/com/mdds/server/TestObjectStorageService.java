/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.server.PresignedUrlAssertions.assertExpiresAtMatchesSignature;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URISyntaxException;
import java.util.UUID;
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

  @Autowired private ObjectStorageService objectStorageService;
  @Autowired private ObjectStorageProperties objectStorageProperties;

  @Container
  private static final MinIOContainer MINIO_CONTAINER =
      new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z")
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
}

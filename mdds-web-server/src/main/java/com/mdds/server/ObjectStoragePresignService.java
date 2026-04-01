/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

/** Here we create an instance to access data from s3. */
@Service
@RequiredArgsConstructor
public class ObjectStoragePresignService {
  private final ObjectStorageProperties objectStorageProperties;
  private S3Presigner preSigner;
  private Duration presignedPutTtl;

  @PostConstruct
  public void init() {
    var region = Region.of(objectStorageProperties.region());
    presignedPutTtl = objectStorageProperties.presignPutTtl();
    var credentialsProvider =
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create(
                objectStorageProperties.accessKey(), objectStorageProperties.secretKey()));

    var publicEndpoint = URI.create(objectStorageProperties.publicEndpoint());

    preSigner =
        S3Presigner.builder()
            .endpointOverride(publicEndpoint)
            .serviceConfiguration(
                S3Configuration.builder()
                    .pathStyleAccessEnabled(objectStorageProperties.pathStyleAccessEnabled())
                    .build())
            .credentialsProvider(credentialsProvider)
            .region(region)
            .build();
  }

  public PresignedUpload issueUploadUrl(long userId, String jobId, String fileName) {
    var bucketName = objectStorageProperties.bucket();
    var objectKey = canonicalInputObjectKey(userId, jobId, fileName);
    var presigned =
        preSigner.presignPutObject(
            b ->
                b.signatureDuration(presignedPutTtl)
                    .putObjectRequest(r -> r.bucket(bucketName).key(objectKey)));
    return new PresignedUpload(presigned.url(), presigned.expiration());
  }

  @PreDestroy
  public void close() {
    if (preSigner != null) {
      preSigner.close();
    }
  }

  private static String canonicalInputObjectKey(long userId, String jobId, String fileName) {
    return "jobs/" + userId + "/" + jobId + "/in/" + fileName;
  }

  public record PresignedUpload(URL uploadUrl, Instant expiresAt) {}
}

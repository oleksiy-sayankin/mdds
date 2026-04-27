/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.common.util.JsonHelper;
import com.mdds.dto.ManifestDTO;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

/** Here we create an instance to access data from s3. */
@Service
@RequiredArgsConstructor
public class ObjectStorageService {
  private final ObjectStorageProperties objectStorageProperties;
  private S3Presigner preSigner;
  private Duration presignedPutTtl;
  private S3Client s3;
  private String bucket;

  @PostConstruct
  public void init() {
    var region = Region.of(objectStorageProperties.region());
    bucket = objectStorageProperties.bucket();
    presignedPutTtl = objectStorageProperties.presignPutTtl();
    var credentialsProvider =
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create(
                objectStorageProperties.accessKey(), objectStorageProperties.secretKey()));

    var publicEndpoint = URI.create(objectStorageProperties.publicEndpoint());
    var internalEndpoint = URI.create(objectStorageProperties.internalEndpoint());
    var s3configuration =
        S3Configuration.builder()
            .pathStyleAccessEnabled(objectStorageProperties.pathStyleAccessEnabled())
            .build();

    preSigner =
        S3Presigner.builder()
            .endpointOverride(publicEndpoint)
            .serviceConfiguration(s3configuration)
            .credentialsProvider(credentialsProvider)
            .region(region)
            .build();
    s3 =
        S3Client.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(internalEndpoint)
            .serviceConfiguration(s3configuration)
            .build();
  }

  public PresignedUpload issueUploadUrl(long userId, String jobId, String fileName) {
    var bucketName = objectStorageProperties.bucket();
    var objectKey = ObjectKeyBuilder.canonicalInputObjectKey(userId, jobId, fileName);
    var presigned =
        preSigner.presignPutObject(
            b ->
                b.signatureDuration(presignedPutTtl)
                    .putObjectRequest(r -> r.bucket(bucketName).key(objectKey)));
    return new PresignedUpload(presigned.url(), presigned.expiration());
  }

  public boolean exists(String key) {
    try {
      var request = HeadObjectRequest.builder().bucket(bucket).key(key).build();
      s3.headObject(request);
      return true;
    } catch (S3Exception e) {
      if (e instanceof NoSuchKeyException || e.statusCode() == HttpStatus.NOT_FOUND.value()) {
        return false;
      }
      throw e;
    }
  }

  void putManifest(String manifestObjectKey, ManifestDTO manifest) {
    var putOb = PutObjectRequest.builder().bucket(bucket).key(manifestObjectKey).build();

    s3.putObject(putOb, RequestBody.fromString(JsonHelper.toJson(manifest)));
  }

  @PreDestroy
  public void close() {
    if (preSigner != null) {
      preSigner.close();
    }
    if (s3 != null) {
      s3.close();
    }
  }

  public record PresignedUpload(URL uploadUrl, Instant expiresAt) {}
}

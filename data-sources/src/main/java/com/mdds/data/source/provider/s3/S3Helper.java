/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.s3;

import com.mdds.api.Processable;
import com.mdds.common.util.JsonHelper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/** Helper class for accessing data from s3. */
public final class S3Helper {
  private S3Helper() {}

  public static ResponseInputStream<GetObjectResponse> getInputStrim(S3Config config, String key) {
    var creds = AwsBasicCredentials.create(config.getAccessKeyId(), config.getSecretAccessKey());
    try (var s3Client =
        S3Client.builder()
            .region(config.getAwsRegion())
            .credentialsProvider(StaticCredentialsProvider.create(creds))
            .build()) {

      var getObjectRequest =
          GetObjectRequest.builder().bucket(config.getBucketName()).key(key).build();
      return s3Client.getObject(getObjectRequest);
    }
  }

  public static Processable<double[][]> extractMatrix(S3Config config) {
    try (var reader =
        new BufferedReader(new InputStreamReader(getInputStrim(config, config.getMatrixKey())))) {
      var sb = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      return Processable.of(JsonHelper.fromJson(sb.toString(), double[][].class));
    } catch (IOException e) {
      return Processable.failure("Can not load matrix from s3", e);
    }
  }

  public static Processable<double[]> extractRhs(S3Config config) {
    try (var reader =
        new BufferedReader(new InputStreamReader(getInputStrim(config, config.getRhsKey())))) {
      var sb = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      return Processable.of(JsonHelper.fromJson(sb.toString(), double[].class));
    } catch (IOException e) {
      return Processable.failure("Can not load right hand side vector from s3", e);
    }
  }
}

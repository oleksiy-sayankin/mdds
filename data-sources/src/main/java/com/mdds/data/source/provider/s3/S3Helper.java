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
import java.net.URI;
import java.net.URISyntaxException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/** Helper class for accessing data from s3. */
public final class S3Helper {
  private S3Helper() {}

  /**
   * Returns input stream for a given file (s3 key) from s3 bucket.
   *
   * @param s3Config contains all parameters to connect to s3 bucket.
   * @param key file to read from s3.
   * @return input stream for a given file (s3 key) from s3 bucket.
   * @throws InvalidS3UrlException when s3 url is invalid. Used for tests with local s3.
   */
  private static ResponseInputStream<GetObjectResponse> getInputStream(
      S3Config s3Config, String key) throws InvalidS3UrlException {
    var s3Creds =
        AwsBasicCredentials.create(s3Config.getAccessKeyId(), s3Config.getSecretAccessKey());
    var s3ServiceConfig =
        S3Configuration.builder()
            .pathStyleAccessEnabled(s3Config.isPathStyleAccessEnabled())
            .build();
    try (var s3Client = buildS3Client(s3Creds, s3ServiceConfig, s3Config)) {
      var getObjectRequest =
          GetObjectRequest.builder().bucket(s3Config.getBucketName()).key(key).build();
      return s3Client.getObject(getObjectRequest);
    } catch (URISyntaxException e) {
      throw new InvalidS3UrlException("Invalid s3 url: " + s3Config.getEndpointUrl(), e);
    }
  }

  private static S3Client buildS3Client(
      AwsBasicCredentials creds, S3Configuration serviceConfig, S3Config config)
      throws URISyntaxException {
    return config.isUseEndpointUrl()
        ? S3Client.builder()
            .region(config.getAwsRegion())
            .endpointOverride(new URI(config.getEndpointUrl()))
            .serviceConfiguration(serviceConfig)
            .credentialsProvider(StaticCredentialsProvider.create(creds))
            .build()
        : S3Client.builder()
            .region(config.getAwsRegion())
            .serviceConfiguration(serviceConfig)
            .credentialsProvider(StaticCredentialsProvider.create(creds))
            .build();
  }

  /**
   * Connects to s3 bucket and gets json file with matrix for SLAE. Parses it and returns 2d array
   * of double values.
   *
   * @param config s3 configuration object.
   * @return matrix of coefficients for SLAE of type double
   */
  public static Processable<double[][]> extractMatrix(S3Config config) {
    try (var reader =
        new BufferedReader(new InputStreamReader(getInputStream(config, config.getMatrixKey())))) {
      var sb = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      return Processable.of(JsonHelper.fromJson(sb.toString(), double[][].class));
    } catch (IOException | InvalidS3UrlException e) {
      return Processable.failure("Can not load matrix from s3", e);
    }
  }

  /**
   * Connects to s3 bucket and gets json file with right hand side vector for SLAE. Parses it and
   * returns array of double values.
   *
   * @param config s3 configuration object.
   * @return right hand side vector for SLAE of type double.
   */
  public static Processable<double[]> extractRhs(S3Config config) {
    try (var reader =
        new BufferedReader(new InputStreamReader(getInputStream(config, config.getRhsKey())))) {
      var sb = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      return Processable.of(JsonHelper.fromJson(sb.toString(), double[].class));
    } catch (IOException | InvalidS3UrlException e) {
      return Processable.failure("Can not load right hand side vector from s3", e);
    }
  }
}

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
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/** Helper class for accessing data from s3. */
public final class S3Helper {
  private S3Helper() {}

  /**
   * Returns input stream for a given file (s3 key) from s3 bucket.
   *
   * @param s3Client client od s3.
   * @param bucket bucket name to connect to s3.
   * @param key file to read from s3.
   * @return input stream for a given file (s3 key) from s3 bucket.
   */
  private static ResponseInputStream<GetObjectResponse> getInputStream(
      S3Client s3Client, String bucket, String key) {
    var getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build();
    return s3Client.getObject(getObjectRequest);
  }

  /**
   * Here we create s3 client and get data from s3 as a stream. Then we read data from s3 and
   * concatenate it as a string. After that we convert it from Json format to Java data types.
   *
   * @param s3Config s3 configuration object.
   * @param key s3 bucket key (file name).
   * @param clazz cass container for destination data type.
   * @param errorMessage output error message when we cannot extract data from s3.
   * @return wrapped into <i>Processable</i> result of the data extraction from s3.
   * @param <T> Java type of the data we want to extract from s3.
   */
  private static <T> Processable<T> extractFromS3(
      S3Config s3Config, String key, Class<T> clazz, String errorMessage) {
    try (var s3Client = S3ClientFactory.create(s3Config);
        var reader =
            new BufferedReader(
                new InputStreamReader(getInputStream(s3Client, s3Config.getBucketName(), key)))) {

      var content = reader.lines().reduce("", String::concat);
      var result = JsonHelper.fromJson(content, clazz);
      return Processable.of(result);

    } catch (IOException | InvalidS3UrlException e) {
      return Processable.failure(errorMessage, e);
    } catch (Exception e) {
      return Processable.failure("Failed to parse JSON from S3: " + e.getMessage(), e);
    }
  }

  /**
   * Connects to s3 bucket and gets json file with matrix for SLAE. Parses it and returns 2d array
   * of double values.
   *
   * @param s3Config s3 configuration object.
   * @return matrix of coefficients for SLAE of type double
   */
  public static Processable<double[][]> extractMatrix(S3Config s3Config) {
    return extractFromS3(
        s3Config, s3Config.getMatrixKey(), double[][].class, "Cannot load matrix from S3");
  }

  /**
   * Connects to s3 bucket and gets json file with right hand side vector for SLAE. Parses it and
   * returns array of double values.
   *
   * @param s3Config s3 configuration object.
   * @return right hand side vector for SLAE of type double.
   */
  public static Processable<double[]> extractRhs(S3Config s3Config) {
    return extractFromS3(
        s3Config,
        s3Config.getRhsKey(),
        double[].class,
        "Cannot load right-hand side vector from S3");
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.s3;

import java.net.URI;
import java.net.URISyntaxException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

/** Factory class for creation s3 client. */
public class S3ClientFactory {
  private S3ClientFactory() {}

  /**
   * Returns s3 client instance.
   *
   * @param config s3 configuration object.
   * @return s3 client.
   * @throws InvalidS3UrlException when s3 url is invalid. Used for tests with local s3.
   */
  public static S3Client create(S3Config config) throws InvalidS3UrlException {
    var creds = AwsBasicCredentials.create(config.getAccessKeyId(), config.getSecretAccessKey());
    var serviceConfig =
        S3Configuration.builder().pathStyleAccessEnabled(config.isPathStyleAccessEnabled()).build();

    var builder =
        S3Client.builder()
            .region(config.getAwsRegion())
            .serviceConfiguration(serviceConfig)
            .credentialsProvider(StaticCredentialsProvider.create(creds));

    if (config.isUseEndpointUrl()) {
      try {
        builder.endpointOverride(new URI(config.getEndpointUrl()));
      } catch (URISyntaxException e) {
        throw new InvalidS3UrlException("Invalid s3 url: ", e);
      }
    }
    return builder.build();
  }
}

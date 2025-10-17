/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.s3;

import java.util.Map;
import lombok.Getter;
import software.amazon.awssdk.regions.Region;

/** Configuration class for s3 data source provider. */
@Getter
public class S3Config {
  private final String bucketName;
  private final String accessKeyId;
  private final String secretAccessKey;
  private final String matrixKey;
  private final String rhsKey;
  private final Region awsRegion;

  private S3Config(Map<String, Object> params) {
    this.bucketName = (String) params.get("aws.bucket.name");
    this.accessKeyId = (String) params.get("aws.access.key.id");
    this.secretAccessKey = (String) params.get("aws.secret.access.key");
    this.matrixKey = (String) params.get("aws.matrix.key");
    this.rhsKey = (String) params.get("aws.rhsKey.key");
    this.awsRegion = (Region) params.get("aws.region");
  }

  public static S3Config of(Map<String, Object> params) {
    return new S3Config(params);
  }
}

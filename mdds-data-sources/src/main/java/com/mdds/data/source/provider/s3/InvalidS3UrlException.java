/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.s3;

/** Thrown when wrong endpoint url is use for s3. */
public class InvalidS3UrlException extends Exception {
  public InvalidS3UrlException(String message, Throwable cause) {
    super(message, cause);
  }
}

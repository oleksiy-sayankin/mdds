/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Indicates illegal (null or blank) upload session id value. */
public class UploadSessionIdIsNullOrBlankException extends RuntimeException {
  public UploadSessionIdIsNullOrBlankException(String message) {
    super(message);
  }
}

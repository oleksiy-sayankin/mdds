/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source;

/** Thrown when we can not upload data, and we point the reason here. */
public class DataUploadException extends Exception {
  public DataUploadException(String message) {
    super(message);
  }
}

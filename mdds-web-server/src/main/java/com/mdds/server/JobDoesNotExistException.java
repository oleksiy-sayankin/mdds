/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Indicates that for requested jobId there is no information in metadata storage. */
public class JobDoesNotExistException extends RuntimeException {
  public JobDoesNotExistException(String message) {
    super(message);
  }
}

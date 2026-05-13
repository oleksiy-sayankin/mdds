/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Indicates that worker status update is invalid. */
public class IllegalJobStatusUpdateException extends RuntimeException {
  public IllegalJobStatusUpdateException(String message) {
    super(message);
  }
}

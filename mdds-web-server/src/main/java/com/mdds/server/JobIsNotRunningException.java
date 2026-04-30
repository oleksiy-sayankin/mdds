/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Indicates that job is not available for cancellation. */
public class JobIsNotRunningException extends RuntimeException {
  public JobIsNotRunningException(String message) {
    super(message);
  }
}

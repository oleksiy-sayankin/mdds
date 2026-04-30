/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Indicates that job is in progress but has no worker. */
public class JobHasNoWorkerAssignedException extends RuntimeException {
  public JobHasNoWorkerAssignedException(String message) {
    super(message);
  }
}

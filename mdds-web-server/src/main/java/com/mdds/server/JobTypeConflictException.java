/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/**
 * Indicates that a draft job already exists for the same (userId, uploadSessionId) but with a
 * different jobType.
 */
public class JobTypeConflictException extends RuntimeException {
  public JobTypeConflictException(String message) {
    super(message);
  }
}

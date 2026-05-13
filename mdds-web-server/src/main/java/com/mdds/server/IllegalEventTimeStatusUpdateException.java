/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Indicates that event time is null when it is required. */
public class IllegalEventTimeStatusUpdateException extends RuntimeException {
  public IllegalEventTimeStatusUpdateException(String message) {
    super(message);
  }
}

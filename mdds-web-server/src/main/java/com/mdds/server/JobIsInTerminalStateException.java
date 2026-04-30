/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Indicates that job is done, cancelled or in error state. */
public class JobIsInTerminalStateException extends RuntimeException {
  public JobIsInTerminalStateException(String message) {
    super(message);
  }
}

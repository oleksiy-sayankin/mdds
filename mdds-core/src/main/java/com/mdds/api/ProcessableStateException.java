/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.api;

/** Throw when we want to get unassigned value from <i>Processable</i> instance. */
public class ProcessableStateException extends RuntimeException {
  public ProcessableStateException(String message, Throwable cause) {
    super(message, cause);
  }
}

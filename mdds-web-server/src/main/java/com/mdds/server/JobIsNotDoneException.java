/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Indicates that the job is not yet in DONE state. */
public class JobIsNotDoneException extends RuntimeException {
  public JobIsNotDoneException(String message) {
    super(message);
  }
}

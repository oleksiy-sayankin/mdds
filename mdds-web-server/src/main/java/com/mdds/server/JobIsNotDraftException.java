/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Indicates that the upload session is already bound to a job that is no longer in DRAFT state. */
public class JobIsNotDraftException extends RuntimeException {
  public JobIsNotDraftException(String message) {
    super(message);
  }
}

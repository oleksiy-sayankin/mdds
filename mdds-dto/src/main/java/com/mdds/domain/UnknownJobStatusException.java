/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

/** Indication that requested job status is unknown or unsupported. */
public class UnknownJobStatusException extends RuntimeException {
  public UnknownJobStatusException(String message) {
    super(message);
  }
}

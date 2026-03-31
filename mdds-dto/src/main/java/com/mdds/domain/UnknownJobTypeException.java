/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

/** Indication that requested job type in unknown or unsupported. */
public class UnknownJobTypeException extends RuntimeException {
  public UnknownJobTypeException(String message) {
    super(message);
  }
}

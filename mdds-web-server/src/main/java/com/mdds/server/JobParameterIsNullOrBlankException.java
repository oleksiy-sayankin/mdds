/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Attempt to set null or blank job parameter. */
public class JobParameterIsNullOrBlankException extends RuntimeException {
  public JobParameterIsNullOrBlankException(String message) {
    super(message);
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/**
 * Indication that requested job type in unknown or unsupported. Job type is not registered in job
 * profiles file.
 */
public class UnknownOrUnsupportedJobTypeException extends RuntimeException {
  public UnknownOrUnsupportedJobTypeException(String message) {
    super(message);
  }
}

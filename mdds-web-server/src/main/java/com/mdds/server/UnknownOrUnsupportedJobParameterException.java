/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/**
 * Requested job parameter name is invalid. We throw this during operation of job parameters
 * replacement.
 */
public class UnknownOrUnsupportedJobParameterException extends RuntimeException {
  public UnknownOrUnsupportedJobParameterException(String message) {
    super(message);
  }
}

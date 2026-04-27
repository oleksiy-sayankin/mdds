/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Required parameter is absent. */
public class RequiredParameterIsAbsentException extends RuntimeException {
  public RequiredParameterIsAbsentException(String message) {
    super(message);
  }
}

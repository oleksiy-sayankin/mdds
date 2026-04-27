/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

/** Indication that requested parameter type is unknown or unsupported. */
public class UnknownParamTypeException extends RuntimeException {
  public UnknownParamTypeException(String message) {
    super(message);
  }
}

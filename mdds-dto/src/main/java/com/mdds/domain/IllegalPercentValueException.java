/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

/** Throw this exception when user wants to set percent value that is not in the range [0; 100]. */
public class IllegalPercentValueException extends RuntimeException {
  public IllegalPercentValueException(String message) {
    super(message);
  }
}

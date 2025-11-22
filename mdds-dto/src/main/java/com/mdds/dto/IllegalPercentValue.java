/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

/** Throw this exception when user wants to set percent value that is not in the range [0; 100]. */
public class IllegalPercentValue extends RuntimeException {
  public IllegalPercentValue(String message) {
    super(message);
  }
}

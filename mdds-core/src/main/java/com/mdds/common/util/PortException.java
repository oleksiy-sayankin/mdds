/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common.util;

/** We throw this when we can not find free port. */
public class PortException extends RuntimeException {
  public PortException(String message, Throwable throwable) {
    super(message, throwable);
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Given slot is not supported for job type. */
public class UnknownOrUnsupportedInputSlotException extends RuntimeException {
  public UnknownOrUnsupportedInputSlotException(String message) {
    super(message);
  }
}

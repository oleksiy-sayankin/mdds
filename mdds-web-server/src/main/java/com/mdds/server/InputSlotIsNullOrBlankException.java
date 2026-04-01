/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Indicates that given input slot is null or blank. */
public class InputSlotIsNullOrBlankException extends RuntimeException {
  public InputSlotIsNullOrBlankException(String message) {
    super(message);
  }
}

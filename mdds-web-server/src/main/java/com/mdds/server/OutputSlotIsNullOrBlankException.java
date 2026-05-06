/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Indicates that given output slot is null or blank. */
public class OutputSlotIsNullOrBlankException extends RuntimeException {
  public OutputSlotIsNullOrBlankException(String message) {
    super(message);
  }
}

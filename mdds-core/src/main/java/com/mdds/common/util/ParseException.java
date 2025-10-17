/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common.util;

/** Thrown when can not read parse double value */
public class ParseException extends RuntimeException {
  public ParseException(String message, Throwable cause) {
    super(message, cause);
  }
}

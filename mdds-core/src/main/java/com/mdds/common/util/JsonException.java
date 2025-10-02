/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common.util;

/** JSON <--> Object convertion exception. */
public class JsonException extends RuntimeException {
  public JsonException(String message, Throwable throwable) {
    super(message, throwable);
  }
}

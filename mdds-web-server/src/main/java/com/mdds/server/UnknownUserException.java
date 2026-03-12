/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** User with given login has not been found. */
public class UnknownUserException extends RuntimeException {
  public UnknownUserException(String message) {
    super(message);
  }
}

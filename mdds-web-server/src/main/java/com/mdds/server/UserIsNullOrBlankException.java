/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Attempt to log in when user is null or empty. */
public class UserIsNullOrBlankException extends RuntimeException {
  public UserIsNullOrBlankException(String message) {
    super(message);
  }
}

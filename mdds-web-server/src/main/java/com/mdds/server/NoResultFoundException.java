/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Indicates that there is no data in data storage. */
public class NoResultFoundException extends RuntimeException {
  public NoResultFoundException(String message) {
    super(message);
  }
}

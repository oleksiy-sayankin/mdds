/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Wrong value of the parameter. */
public class InvalidJobParameterValueException extends RuntimeException {
  public InvalidJobParameterValueException(String message) {
    super(message);
  }
}

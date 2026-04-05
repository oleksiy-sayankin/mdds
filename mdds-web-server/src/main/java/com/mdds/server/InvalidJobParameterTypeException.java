/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Input job parameter type does not correspond to that one in job profile. */
public class InvalidJobParameterTypeException extends RuntimeException {
  public InvalidJobParameterTypeException(String message) {
    super(message);
  }
}

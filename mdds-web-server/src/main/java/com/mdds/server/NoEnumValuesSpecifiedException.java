/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

/** Indicates that we created an enum but did not specify values for it. */
public class NoEnumValuesSpecifiedException extends RuntimeException {
  public NoEnumValuesSpecifiedException(String message) {
    super(message);
  }
}

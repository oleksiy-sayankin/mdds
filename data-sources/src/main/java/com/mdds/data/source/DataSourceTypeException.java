/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source;

/** Throw when we can not parse data source type. */
public class DataSourceTypeException extends RuntimeException {
  public DataSourceTypeException(String message) {
    super(message);
  }
}

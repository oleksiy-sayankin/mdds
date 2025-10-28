/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider.mysql;

/** Throw when there is ni data found in Db table. */
public class NoDataFoundException extends Exception {
  public NoDataFoundException(String message) {
    super(message);
  }
}

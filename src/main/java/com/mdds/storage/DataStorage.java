/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

package com.mdds.storage;

/** Represents common key value data storage such as Redis, MongoDB ect. */
public interface DataStorage {
  void connect();

  void put(String key, Object value);

  <T> T get(String key, Class<T> type);

  void close();
}

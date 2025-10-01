/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

package com.mdds.storage;

import java.util.Optional;

/** Represents common key value data storage such as Redis, MongoDB ect. */
public interface DataStorage extends AutoCloseable {

  /**
   * Puts key/value pair to storage.
   *
   * @param key key to put.
   * @param value value to put.
   * @param <T> common type of the value.
   */
  <T> void put(String key, T value);

  /**
   * Gets value from storage by known key.
   *
   * @param key known key to get value.
   * @param type type of the value.
   * @return value from the storage.
   * @param <T> destination class type for the value.
   */
  <T> Optional<T> get(String key, Class<T> type);

  /** Closes connection (if any) to storage. */
  @Override
  void close();
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

package com.mdds;

import dto.ResultDTO;

/** Represents common key value data storage such as Redis, MongoDB ect. */
public interface DataStorage {
  void connect();

  void put(String key, ResultDTO value);

  ResultDTO get(String key);

  void close();
}

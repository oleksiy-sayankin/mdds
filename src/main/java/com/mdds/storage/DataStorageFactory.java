/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage;

import com.mdds.storage.redis.RedisDataStorage;

/** Factory class for data storages. */
public final class DataStorageFactory {
  private DataStorageFactory() {}

  /**
   * Creates Redis Data Storage. Reads host and port from redis.properties file.
   *
   * @return Redis Data Storage.
   */
  public static DataStorage createRedis() {
    return new RedisDataStorage();
  }

  /**
   * Creates Redis Data Storage.
   *
   * @param host Redis host.
   * @param port Redis port.
   * @return Connected Redis Data storage.
   */
  public static DataStorage createRedis(String host, int port) {
    return new RedisDataStorage(host, port);
  }
}

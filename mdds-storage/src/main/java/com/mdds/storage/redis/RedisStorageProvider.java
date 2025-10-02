/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage.redis;

import com.mdds.storage.DataStorage;
import com.mdds.storage.DataStorageFactory;
import com.mdds.storage.StorageProvider;
import jakarta.annotation.Nonnull;

/** Provider for Redis Data Storage. */
public class RedisStorageProvider implements StorageProvider {
  /**
   * Creates Redis Data Storage configuration and an instance of Redis Data Storage connection.
   *
   * @return instance of Redis Data Storage connection.
   */
  @Override
  public @Nonnull DataStorage get() {
    return DataStorageFactory.createRedis(RedisConfFactory.fromEnvOrDefaultProperties());
  }
}

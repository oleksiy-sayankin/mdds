/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.storage;

import jakarta.annotation.Nonnull;

/** Basic interface for getting Data Storage instance. */
public interface StorageProvider {
  /**
   * Creates configuration for Data Storage and connects to Data Storage with that configuration.
   *
   * @return Data Storage instance.
   */
  @Nonnull
  DataStorage get();
}

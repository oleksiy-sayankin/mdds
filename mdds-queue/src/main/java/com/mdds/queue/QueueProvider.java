/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue;

import jakarta.annotation.Nonnull;

/** Basic interface for getting Queue instance. */
public interface QueueProvider {
  /**
   * Creates configuration for Queue and connects to Queue with that configuration.
   *
   * @return Queue instance
   */
  @Nonnull
  Queue get();
}

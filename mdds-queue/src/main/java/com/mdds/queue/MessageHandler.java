/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue;

import jakarta.annotation.Nonnull;

/** Provides callback (Acknowledger) for handling queue messages. */
@FunctionalInterface
public interface MessageHandler<T> {
  /**
   * Basic method to process message obtained from the queue.
   *
   * @param message message that we want to process
   * @param ack an interface that implements two methods: how to inform the queue that message is
   *     processed and how to reject message in case of error.
   */
  void handle(@Nonnull Message<T> message, @Nonnull Acknowledger ack);
}

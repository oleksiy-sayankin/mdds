/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

package com.mdds.queue;

import jakarta.annotation.Nonnull;

/** Common interface for Job Queue and Result Queue. */
public interface Queue extends AutoCloseable {
  /**
   * Publishes message to queue.
   *
   * @param queueName where we want to publish message
   * @param message what we want to publish
   * @param <T> what class type we use as payload in message.
   */
  <T> void publish(@Nonnull String queueName, @Nonnull Message<T> message);

  /**
   * Subscribes to the queue and processes messages from the queue.
   *
   * @param <T> What exact class we use as payload.
   * @param queueName what queue we want to subscribe.
   * @param payloadType class that we use for payload. It is not payload itself, it indicates to
   *     what tipe we convert deserialized data when we get them from the queue.
   * @param handler what we do, when we obtain message from queue.
   * @return subscription object. This object only can be closed after queue is not used anymore. We
   *     work with subscription object in a manner of try-with-resources.
   */
  <T> @Nonnull Subscription subscribe(
      @Nonnull String queueName, @Nonnull Class<T> payloadType, @Nonnull MessageHandler<T> handler);

  /**
   * Deletes queue.
   *
   * @param queueName queue to delete.
   */
  void deleteQueue(@Nonnull String queueName);

  /** Closes Queue and releases connection if any. */
  @Override
  void close();
}

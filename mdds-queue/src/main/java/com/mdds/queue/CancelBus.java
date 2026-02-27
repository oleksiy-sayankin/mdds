/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue;

import com.mdds.dto.CancelJobDTO;
import jakarta.annotation.Nonnull;

/** Bus for cancelling jobs that runs on a particular executor. */
public interface CancelBus {
  /**
   * Sends cancel request to the Receiver. Here the Receiver is the Executor.
   *
   * @param executorId unique Executor identifier, where to cancel a job
   * @param message unique job identifier
   */
  void sendCancel(@Nonnull String executorId, @Nonnull Message<CancelJobDTO> message);

  /**
   * Subscribes to cancel bus and returns subscription.
   *
   * @param executorId unique Executor identifier, where to cancel a job
   * @param handler handler to process cancel request
   * @return subscription to cancel bus
   */
  Subscription subscribe(@Nonnull String executorId, @Nonnull MessageHandler<CancelJobDTO> handler);
}

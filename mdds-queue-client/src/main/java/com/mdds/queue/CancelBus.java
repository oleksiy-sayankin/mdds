/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue;

import com.mdds.dto.rest.v1.CancelJobRequestDTO;
import jakarta.annotation.Nonnull;

/** Bus for cancelling jobs running on a particular Worker. */
public interface CancelBus {
  /**
   * Sends cancel request to the Receiver. Here the Receiver is the Worker.
   *
   * @param workerId unique Worker identifier, where to cancel a job
   * @param message cancellation message containing the job identifier
   */
  void sendCancel(@Nonnull String workerId, @Nonnull Message<CancelJobRequestDTO> message);

  /**
   * Subscribes to cancel bus and returns subscription.
   *
   * @param workerId unique Worker identifier, where to cancel a job
   * @param handler handler to process cancel request
   * @return subscription to cancel bus
   */
  Subscription subscribe(
      @Nonnull String workerId, @Nonnull MessageHandler<CancelJobRequestDTO> handler);
}

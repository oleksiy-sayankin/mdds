/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue;

/** Here we inform the Queue that is processed (ack) or not processed (nack). */
public interface Acknowledger {
  /** Acknowledge that message is processed. */
  void ack();

  /**
   * Rejects message from queue.
   *
   * @param requeue true if we want to put message to queue again.
   */
  void nack(boolean requeue);
}

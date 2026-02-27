/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue.rabbitmq;

import com.mdds.dto.CancelJobDTO;
import com.mdds.queue.CancelBus;
import com.mdds.queue.CancelDestinationResolver;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import com.mdds.queue.Queue;
import com.mdds.queue.Subscription;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;

/** Bus for cancelling a job running on an executor. */
@Slf4j
public class RabbitMqCancelBus implements CancelBus {

  private final Queue cancelQueue;
  private final CancelDestinationResolver resolver;

  public RabbitMqCancelBus(Queue cancelQueue, CancelDestinationResolver resolver) {
    this.cancelQueue = cancelQueue;
    this.resolver = resolver;
  }

  @Override
  public void sendCancel(@NonNull String executorId, @NonNull Message<CancelJobDTO> message) {
    cancelQueue.publish(resolver.destinationFor(executorId), message);
  }

  @Override
  public Subscription subscribe(
      @Nonnull String executorId, @Nonnull MessageHandler<CancelJobDTO> handler) {
    return cancelQueue.subscribe(resolver.destinationFor(executorId), CancelJobDTO.class, handler);
  }
}

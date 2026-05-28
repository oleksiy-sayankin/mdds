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
import com.mdds.queue.QueueClient;
import com.mdds.queue.Subscription;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;

/** Bus for cancelling a job running on an executor. */
@Slf4j
public class RabbitMqCancelBus implements CancelBus {

  private final QueueClient cancelQueueClient;
  private final CancelDestinationResolver resolver;

  public RabbitMqCancelBus(QueueClient cancelQueueClient, CancelDestinationResolver resolver) {
    this.cancelQueueClient = cancelQueueClient;
    this.resolver = resolver;
  }

  @Override
  public void sendCancel(@NonNull String executorId, @NonNull Message<CancelJobDTO> message) {
    cancelQueueClient.publish(resolver.destinationFor(executorId), message);
  }

  @Override
  public Subscription subscribe(
      @Nonnull String executorId, @Nonnull MessageHandler<CancelJobDTO> handler) {
    return cancelQueueClient.subscribe(
        resolver.destinationFor(executorId), CancelJobDTO.class, handler);
  }
}

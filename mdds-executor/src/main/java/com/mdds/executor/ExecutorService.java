/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import static com.mdds.common.AppConstants.TASK_QUEUE_NAME;

import com.mdds.common.AppConstantsFactory;
import com.mdds.dto.TaskDTO;
import com.mdds.queue.MessageHandler;
import com.mdds.queue.Queue;
import com.mdds.queue.Subscription;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Service that creates subscription to Task Queue where it takes message from Task Queue and solves
 * system of linear equations. After that this service puts results to Result Queue.
 */
@Slf4j
public class ExecutorService implements AutoCloseable {
  private final @Nonnull Queue taskQueue;
  private final @Nonnull Queue resultQueue;
  private final @Nonnull Subscription taskQueueSubscription;

  public ExecutorService(
      @Nonnull Queue taskQueue,
      @Nonnull Queue resultQueue,
      @Nonnull MessageHandler<TaskDTO> messageHandler) {
    this.taskQueue = taskQueue;
    this.resultQueue = resultQueue;
    this.taskQueueSubscription =
        taskQueue.subscribe(
            AppConstantsFactory.getString(TASK_QUEUE_NAME), TaskDTO.class, messageHandler);
    log.info(
        "ExecutorService started with task queue = {}, result queue = {}", taskQueue, resultQueue);
  }

  @Override
  public void close() {
    taskQueueSubscription.close();
    taskQueue.close();
    resultQueue.close();
    log.info("ExecutorService shut down cleanly");
  }
}

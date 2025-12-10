/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import com.google.common.annotations.VisibleForTesting;
import com.mdds.common.CommonProperties;
import com.mdds.dto.TaskDTO;
import com.mdds.queue.MessageHandler;
import com.mdds.queue.Queue;
import com.mdds.queue.Subscription;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Service that creates subscription to Task Queue where it takes message from Task Queue and solves
 * system of linear equations. After that this service puts results to Result Queue.
 */
@Slf4j
@Service
public class ExecutorService implements AutoCloseable {
  private final Queue taskQueue;
  private final Queue resultQueue;
  private final MessageHandler<TaskDTO> messageHandler;
  private final CommonProperties commonProperties;
  private Subscription taskQueueSubscription;

  @Autowired
  public ExecutorService(
      @Qualifier("taskQueue") Queue taskQueue,
      @Qualifier("resultQueue") Queue resultQueue,
      MessageHandler<TaskDTO> messageHandler,
      CommonProperties commonProperties) {
    this.taskQueue = taskQueue;
    this.resultQueue = resultQueue;
    this.messageHandler = messageHandler;
    this.commonProperties = commonProperties;
    log.info(
        "Created Executor Service with '{}', {}, '{}' {}, {}",
        commonProperties.getTaskQueueName(),
        taskQueue,
        commonProperties.getResultQueueName(),
        resultQueue,
        messageHandler);
  }

  @VisibleForTesting
  public ExecutorService(
      Queue taskQueue,
      Queue resultQueue,
      MessageHandler<TaskDTO> messageHandler,
      Subscription taskQueueSubscription,
      CommonProperties commonProperties) {
    this.taskQueue = taskQueue;
    this.resultQueue = resultQueue;
    this.messageHandler = messageHandler;
    this.taskQueueSubscription = taskQueueSubscription;
    this.commonProperties = commonProperties;
  }

  @PostConstruct
  public void start() {
    this.taskQueueSubscription =
        taskQueue.subscribe(commonProperties.getTaskQueueName(), TaskDTO.class, messageHandler);
    log.info(
        "Executor Service started with task queue '{}', {}, result queue '{}', {}",
        commonProperties.getTaskQueueName(),
        taskQueue,
        commonProperties.getResultQueueName(),
        resultQueue);
  }

  @PreDestroy
  @Override
  public void close() {
    taskQueueSubscription.close();
    taskQueue.close();
    resultQueue.close();
    log.info("ExecutorService shut down cleanly");
  }
}

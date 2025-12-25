/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import com.google.common.annotations.VisibleForTesting;
import com.mdds.common.CommonProperties;
import com.mdds.dto.CancelTaskDTO;
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
  private final Queue cancelQueue;
  private final MessageHandler<CancelTaskDTO> cancelMessageHandler;
  private final CommonProperties commonProperties;
  private final ExecutorProperties executorProperties;
  private Subscription taskQueueSubscription;
  private Subscription cancelQueueSubscription;

  @Autowired
  public ExecutorService(
      @Qualifier("taskQueue") Queue taskQueue,
      @Qualifier("resultQueue") Queue resultQueue,
      MessageHandler<TaskDTO> messageHandler,
      @Qualifier("cancelQueue") Queue cancelQueue,
      MessageHandler<CancelTaskDTO> cancelMessageHandler,
      CommonProperties commonProperties,
      ExecutorProperties executorProperties) {
    this.taskQueue = taskQueue;
    this.resultQueue = resultQueue;
    this.messageHandler = messageHandler;
    this.cancelQueue = cancelQueue;
    this.cancelMessageHandler = cancelMessageHandler;
    this.commonProperties = commonProperties;
    this.executorProperties = executorProperties;
    log.info(
        "Created Executor Service with '{}', {}, '{}' {}, {}, {}, {}",
        commonProperties.getTaskQueueName(),
        taskQueue,
        commonProperties.getResultQueueName(),
        resultQueue,
        messageHandler,
        executorProperties.getCancelQueueName(),
        cancelMessageHandler);
  }

  @VisibleForTesting
  public ExecutorService(
      Queue taskQueue,
      Queue resultQueue,
      Queue cancelQueue,
      MessageHandler<TaskDTO> messageHandler,
      MessageHandler<CancelTaskDTO> cancelMessageHandler,
      Subscription taskQueueSubscription,
      Subscription cancelQueueSubscription,
      CommonProperties commonProperties,
      ExecutorProperties executorProperties) {
    this.taskQueue = taskQueue;
    this.resultQueue = resultQueue;
    this.cancelQueue = cancelQueue;
    this.messageHandler = messageHandler;
    this.cancelMessageHandler = cancelMessageHandler;
    this.taskQueueSubscription = taskQueueSubscription;
    this.cancelQueueSubscription = cancelQueueSubscription;
    this.commonProperties = commonProperties;
    this.executorProperties = executorProperties;
  }

  @PostConstruct
  public void start() {
    this.taskQueueSubscription =
        taskQueue.subscribe(commonProperties.getTaskQueueName(), TaskDTO.class, messageHandler);
    this.cancelQueueSubscription =
        cancelQueue.subscribe(
            executorProperties.getCancelQueueName(), CancelTaskDTO.class, cancelMessageHandler);
    log.info(
        "Executor Service started with task queue '{}', {}, result queue '{}', {}, cancel queue"
            + " '{}', {}",
        commonProperties.getTaskQueueName(),
        taskQueue,
        commonProperties.getResultQueueName(),
        resultQueue,
        executorProperties.getCancelQueueName(),
        cancelQueue);
  }

  @PreDestroy
  @Override
  public void close() {
    taskQueueSubscription.close();
    cancelQueueSubscription.close();
    taskQueue.close();
    resultQueue.close();
    cancelQueue.close();
    log.info("ExecutorService shut down cleanly");
  }
}

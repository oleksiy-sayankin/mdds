/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import com.google.common.annotations.VisibleForTesting;
import com.mdds.common.CommonProperties;
import com.mdds.dto.CancelJobDTO;
import com.mdds.dto.JobDTO;
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
 * Service that creates subscription to Job Queue where it takes message from Job Queue and solves
 * system of linear equations. After that this service puts results to Result Queue.
 */
@Slf4j
@Service
public class ExecutorService implements AutoCloseable {
  private final Queue jobQueue;
  private final Queue resultQueue;
  private final MessageHandler<JobDTO> messageHandler;
  private final Queue cancelQueue;
  private final MessageHandler<CancelJobDTO> cancelMessageHandler;
  private final CommonProperties commonProperties;
  private final ExecutorProperties executorProperties;
  private Subscription jobQueueSubscription;
  private Subscription cancelQueueSubscription;

  @Autowired
  public ExecutorService(
      @Qualifier("jobQueue") Queue jobQueue,
      @Qualifier("resultQueue") Queue resultQueue,
      MessageHandler<JobDTO> messageHandler,
      @Qualifier("cancelQueue") Queue cancelQueue,
      MessageHandler<CancelJobDTO> cancelMessageHandler,
      CommonProperties commonProperties,
      ExecutorProperties executorProperties) {
    this.jobQueue = jobQueue;
    this.resultQueue = resultQueue;
    this.messageHandler = messageHandler;
    this.cancelQueue = cancelQueue;
    this.cancelMessageHandler = cancelMessageHandler;
    this.commonProperties = commonProperties;
    this.executorProperties = executorProperties;
    log.info(
        "Created Executor Service with '{}', {}, '{}' {}, {}, {}, {}",
        commonProperties.getJobQueueName(),
        jobQueue,
        commonProperties.getResultQueueName(),
        resultQueue,
        messageHandler,
        executorProperties.getCancelQueueName(),
        cancelMessageHandler);
  }

  @VisibleForTesting
  public ExecutorService(
      Queue jobQueue,
      Queue resultQueue,
      Queue cancelQueue,
      MessageHandler<JobDTO> messageHandler,
      MessageHandler<CancelJobDTO> cancelMessageHandler,
      Subscription jobQueueSubscription,
      Subscription cancelQueueSubscription,
      CommonProperties commonProperties,
      ExecutorProperties executorProperties) {
    this.jobQueue = jobQueue;
    this.resultQueue = resultQueue;
    this.cancelQueue = cancelQueue;
    this.messageHandler = messageHandler;
    this.cancelMessageHandler = cancelMessageHandler;
    this.jobQueueSubscription = jobQueueSubscription;
    this.cancelQueueSubscription = cancelQueueSubscription;
    this.commonProperties = commonProperties;
    this.executorProperties = executorProperties;
  }

  @PostConstruct
  public void start() {
    this.jobQueueSubscription =
        jobQueue.subscribe(commonProperties.getJobQueueName(), JobDTO.class, messageHandler);
    this.cancelQueueSubscription =
        cancelQueue.subscribe(
            executorProperties.getCancelQueueName(), CancelJobDTO.class, cancelMessageHandler);
    log.info(
        "Executor Service started with job queue '{}', {}, result queue '{}', {}, cancel queue"
            + " '{}', {}",
        commonProperties.getJobQueueName(),
        jobQueue,
        commonProperties.getResultQueueName(),
        resultQueue,
        executorProperties.getCancelQueueName(),
        cancelQueue);
  }

  @PreDestroy
  @Override
  public void close() {
    jobQueueSubscription.close();
    cancelQueueSubscription.close();
    jobQueue.close();
    resultQueue.close();
    cancelQueue.close();
    log.info("ExecutorService shut down cleanly");
  }
}

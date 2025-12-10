/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.result;

import com.mdds.common.CommonProperties;
import com.mdds.dto.ResultDTO;
import com.mdds.queue.Queue;
import com.mdds.queue.Subscription;
import com.mdds.storage.DataStorage;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Service that creates subscription to Queue where it takes message from Queue and puts it to
 * Storage.
 */
@Slf4j
@Service
public class ResultConsumerService implements AutoCloseable {
  private final DataStorage dataStorage;
  private final Queue queue;
  private final CommonProperties commonProperties;
  private Subscription subscription;

  @Autowired
  public ResultConsumerService(
      DataStorage dataStorage,
      @Qualifier("resultQueue") Queue queue,
      CommonProperties commonProperties) {
    this.dataStorage = dataStorage;
    this.queue = queue;
    this.commonProperties = commonProperties;
    log.info(
        "Created Result Consumer Service {}, '{}' {}",
        dataStorage,
        commonProperties.getResultQueueName(),
        queue);
  }

  @PostConstruct
  public void start() {
    subscription =
        queue.subscribe(
            commonProperties.getResultQueueName(),
            ResultDTO.class,
            (message, ack) -> {
              var payload = message.payload();
              dataStorage.put(payload.getTaskId(), payload);
              ack.ack();
              log.info("Stored result for task {} to storage {}", payload.getTaskId(), dataStorage);
            });
    log.info(
        "Started Result Consumer Service with queue '{}' = {} and storage {}.",
        commonProperties.getResultQueueName(),
        queue,
        dataStorage);
  }

  @PreDestroy
  @Override
  public void close() {
    subscription.close();
    queue.close();
    dataStorage.close();
    log.info("Result Consumer Service shut down cleanly");
  }
}

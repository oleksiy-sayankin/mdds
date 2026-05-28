/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.result;

import com.mdds.common.CommonProperties;
import com.mdds.dto.ResultDTO;
import com.mdds.queue.QueueClient;
import com.mdds.queue.Subscription;
import com.mdds.storage.DataStorage;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
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
  private final QueueClient queueClient;
  private final CommonProperties commonProperties;
  private Subscription subscription;

  @Autowired
  public ResultConsumerService(
      DataStorage dataStorage,
      @Qualifier("resultQueueClient") QueueClient queueClient,
      CommonProperties commonProperties) {
    this.dataStorage = dataStorage;
    this.queueClient = queueClient;
    this.commonProperties = commonProperties;
    log.info(
        "Created Result Consumer Service {}, '{}' {}",
        dataStorage,
        commonProperties.getResultQueueName(),
        queueClient);
  }

  @PostConstruct
  public void start() {
    subscription =
        queueClient.subscribe(
            commonProperties.getResultQueueName(),
            ResultDTO.class,
            (message, ack) -> {
              var payload = message.payload();
              try (var ignoredJobId = MDC.putCloseable("jobId", payload.getJobId());
                  var ignoredEvent = MDC.putCloseable("event", "store_result")) {
                dataStorage.put(payload.getJobId(), payload);
                ack.ack();
                log.info("Stored result for job to storage {}", dataStorage);
              }
            });
    log.info(
        "Started Result Consumer Service with queue '{}' = {} and storage {}.",
        commonProperties.getResultQueueName(),
        queueClient,
        dataStorage);
  }

  @PreDestroy
  @Override
  public void close() {
    subscription.close();
    queueClient.close();
    dataStorage.close();
    log.info("Result Consumer Service shut down cleanly");
  }
}

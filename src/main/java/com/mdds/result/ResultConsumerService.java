/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.result;

import static com.mdds.common.AppConstants.RESULT_QUEUE_NAME;

import com.mdds.common.AppConstantsFactory;
import com.mdds.queue.Queue;
import com.mdds.queue.Subscription;
import com.mdds.storage.DataStorage;
import dto.ResultDTO;
import lombok.extern.slf4j.Slf4j;

/**
 * Service that creates subscription to Queue where it takes message from Queue and puts it to
 * Storage.
 */
@Slf4j
public class ResultConsumerService implements AutoCloseable {
  private final DataStorage dataStorage;
  private final Queue queue;
  private final Subscription subscription;

  public ResultConsumerService(DataStorage dataStorage, Queue queue) {
    this.dataStorage = dataStorage;
    this.queue = queue;
    this.subscription =
        queue.subscribe(
            AppConstantsFactory.getString(RESULT_QUEUE_NAME),
            ResultDTO.class,
            (message, ack) -> {
              var payload = message.payload();
              dataStorage.put(payload.getTaskId(), payload);
              ack.ack();
              log.debug("Stored result for task {}", payload.getTaskId());
            });
  }

  @Override
  public void close() {
    if (subscription != null) subscription.close();
    if (queue != null) queue.close();
    if (dataStorage != null) dataStorage.close();
    log.info("ResultConsumerService shut down cleanly");
  }
}

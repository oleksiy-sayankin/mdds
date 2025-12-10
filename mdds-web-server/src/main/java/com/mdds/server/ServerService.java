/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.queue.Queue;
import com.mdds.storage.DataStorage;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Slf4j
@Getter
@Service
public class ServerService implements AutoCloseable {
  private final DataStorage dataStorage;
  private final Queue taskQueue;

  @Autowired
  public ServerService(DataStorage dataStorage, @Qualifier("taskQueue") Queue taskQueue) {
    this.dataStorage = dataStorage;
    this.taskQueue = taskQueue;
    log.info(
        "Constructed Server Service with task queue {} and storage {}", taskQueue, dataStorage);
  }

  @PreDestroy
  @Override
  public void close() {
    taskQueue.close();
    dataStorage.close();
    log.info("Web Server Service shut down cleanly");
  }
}

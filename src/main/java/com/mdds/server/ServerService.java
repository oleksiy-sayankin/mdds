/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.queue.Queue;
import com.mdds.storage.DataStorage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class ServerService implements AutoCloseable {
  private final DataStorage dataStorage;
  private final Queue queue;

  public ServerService(DataStorage dataStorage, Queue queue) {
    this.dataStorage = dataStorage;
    this.queue = queue;
  }

  @Override
  public void close() {
    if (queue != null) queue.close();
    if (dataStorage != null) dataStorage.close();
    log.info("ResultConsumerService shut down cleanly");
  }
}

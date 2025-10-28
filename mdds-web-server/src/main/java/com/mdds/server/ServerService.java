/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.queue.Queue;
import com.mdds.storage.DataStorage;
import jakarta.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class ServerService implements AutoCloseable {
  private final @Nonnull DataStorage dataStorage;
  private final @Nonnull Queue queue;

  public ServerService(@Nonnull DataStorage dataStorage, @Nonnull Queue queue) {
    this.dataStorage = dataStorage;
    this.queue = queue;
  }

  @Override
  public void close() {
    queue.close();
    dataStorage.close();
    log.info("ResultConsumerService shut down cleanly");
  }
}

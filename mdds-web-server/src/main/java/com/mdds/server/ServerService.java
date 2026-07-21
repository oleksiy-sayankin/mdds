/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.queue.QueueClient;
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
  private final QueueClient jobQueueClient;

  @Autowired
  public ServerService(@Qualifier("jobQueueClient") QueueClient jobQueueClient) {
    this.jobQueueClient = jobQueueClient;
    log.info("Constructed Server Service with job queue {}", jobQueueClient);
  }

  @PreDestroy
  @Override
  public void close() {
    jobQueueClient.close();
    log.info("Web Server Service shut down cleanly");
  }
}

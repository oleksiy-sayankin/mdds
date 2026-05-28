/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.common.CommonProperties;
import com.mdds.dto.JobStatusUpdateDTO;
import com.mdds.queue.QueueClient;
import com.mdds.queue.Subscription;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Service that creates subscription to status Queue where it takes message from Queue and puts it
 * to Metadata Storage.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StatusManagerService {
  private Subscription subscription;
  private final @Qualifier("statusQueueClient") QueueClient queueClient;
  private final CommonProperties commonProperties;
  private final JobStatusUpdateService jobStatusUpdateService;

  private static final String JOB_ID = "jobId";
  private static final String USER_ID = "userId";
  private static final String EVENT = "event";

  @PostConstruct
  public void start() {
    subscription =
        queueClient.subscribe(
            commonProperties.getStatusQueueName(),
            JobStatusUpdateDTO.class,
            (message, ack) -> {
              var payload = message == null ? null : message.payload();
              var jobId =
                  Optional.ofNullable(payload).map(JobStatusUpdateDTO::jobId).orElse("<null>");
              try (var ignoredJobId = MDC.putCloseable(JOB_ID, jobId);
                  var ignoredEvent = MDC.putCloseable(EVENT, "persist_status")) {

                var result = jobStatusUpdateService.apply(payload);

                try (var ignoredUserId =
                    MDC.putCloseable(USER_ID, Long.toString(result.userId()))) {
                  ack.ack();
                  log.info(
                      "Stored status '{}' for job to metadata storage.", result.status().getCode());
                }
              } catch (Exception e) {
                log.error("Failed to persist status update for job '{}'.", jobId, e);
                ack.nack(false);
              }
            });
    log.info(
        "Started Status Manager Service with queue '{}' = {}.",
        commonProperties.getStatusQueueName(),
        queueClient);
  }

  @PreDestroy
  public void close() {
    Optional.ofNullable(subscription).ifPresent(Subscription::close);
    queueClient.close();
    log.info("Status Manager Service shut down cleanly");
  }
}

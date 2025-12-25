/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import com.mdds.dto.CancelTaskDTO;
import com.mdds.queue.Acknowledger;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Component;

/**
 * Cancels task by task id. {@link ExecutorMessageHandler} stores all open gRPC sessions and can
 * terminate one by request.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CancelMessageHandler implements MessageHandler<CancelTaskDTO> {
  private final ExecutorMessageHandler executorMessageHandler;

  @Override
  public void handle(@NonNull Message<CancelTaskDTO> message, @NonNull Acknowledger ack) {
    var taskId = message.payload().getTaskId();
    var ok = executorMessageHandler.cancelTask(taskId);
    log.info("Cancel request: taskId={}, cancelled={}", taskId, ok);
    ack.ack();
  }
}

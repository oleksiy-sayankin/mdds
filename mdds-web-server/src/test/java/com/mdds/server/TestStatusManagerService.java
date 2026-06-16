/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.domain.JobStatus.IN_PROGRESS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mdds.common.CommonProperties;
import com.mdds.dto.JobStatusUpdateDTO;
import com.mdds.queue.Acknowledger;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import com.mdds.queue.QueueClient;
import com.mdds.queue.Subscription;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestStatusManagerService {

  private static final Instant BASE_EVENT_TIME = Instant.parse("2026-01-01T00:00:00Z");

  @Test
  void testProcessStatusUpdateMessage() {
    var statusQueue = mock(QueueClient.class);
    var commonProperties = mock(CommonProperties.class);
    var jobStatusUpdateService = mock(JobStatusUpdateService.class);
    var subscription = mock(Subscription.class);

    when(commonProperties.getStatusQueueName()).thenReturn("status.queue.test");
    when(statusQueue.subscribe(eq("status.queue.test"), eq(JobStatusUpdateDTO.class), any()))
        .thenReturn(subscription);

    var statusManagerService =
        new StatusManagerService(statusQueue, commonProperties, jobStatusUpdateService);

    statusManagerService.start();

    @SuppressWarnings({"unchecked"})
    ArgumentCaptor<MessageHandler<JobStatusUpdateDTO>> handlerCaptor =
        ArgumentCaptor.forClass(MessageHandler.class);

    verify(statusQueue)
        .subscribe(eq("status.queue.test"), eq(JobStatusUpdateDTO.class), handlerCaptor.capture());

    var handler = handlerCaptor.getValue();
    var ack = mock(Acknowledger.class);

    var eventTime = BASE_EVENT_TIME;
    var payload =
        new JobStatusUpdateDTO(
            "job-1", "worker-1", IN_PROGRESS.getCode(), 10, "Started processing", eventTime);

    var queueMessage = new Message<>(payload, Map.of(), eventTime);

    when(jobStatusUpdateService.apply(payload))
        .thenReturn(new JobStatusUpdateService.JobStatusUpdateResult("job-1", 100L, IN_PROGRESS));

    handler.handle(queueMessage, ack);

    verify(jobStatusUpdateService).apply(payload);
    verify(ack).ack();
    verify(ack, never()).nack(anyBoolean());
  }

  @Test
  void testApplyThrowsException() {
    var statusQueue = mock(QueueClient.class);
    var commonProperties = mock(CommonProperties.class);
    var jobStatusUpdateService = mock(JobStatusUpdateService.class);
    var subscription = mock(Subscription.class);

    when(commonProperties.getStatusQueueName()).thenReturn("status.queue.test");
    when(statusQueue.subscribe(eq("status.queue.test"), eq(JobStatusUpdateDTO.class), any()))
        .thenReturn(subscription);

    var statusManagerService =
        new StatusManagerService(statusQueue, commonProperties, jobStatusUpdateService);

    statusManagerService.start();

    @SuppressWarnings({"unchecked"})
    ArgumentCaptor<MessageHandler<JobStatusUpdateDTO>> handlerCaptor =
        ArgumentCaptor.forClass(MessageHandler.class);

    verify(statusQueue)
        .subscribe(eq("status.queue.test"), eq(JobStatusUpdateDTO.class), handlerCaptor.capture());

    var handler = handlerCaptor.getValue();
    var ack = mock(Acknowledger.class);

    var eventTime = BASE_EVENT_TIME;
    var jobId = "invalid-job-id";
    var payload =
        new JobStatusUpdateDTO(
            jobId, "worker-1", IN_PROGRESS.getCode(), 10, "Started processing", eventTime);

    var queueMessage = new Message<>(payload, Map.of(), eventTime);

    when(jobStatusUpdateService.apply(payload))
        .thenThrow(new JobDoesNotExistException("Job with id '" + jobId + "' does not exist."));
    handler.handle(queueMessage, ack);
    verify(ack).nack(false);
    verify(ack, never()).ack();
  }

  @Test
  void testNullPayload() {
    var statusQueue = mock(QueueClient.class);
    var commonProperties = mock(CommonProperties.class);
    var jobStatusUpdateService = mock(JobStatusUpdateService.class);
    var subscription = mock(Subscription.class);

    when(commonProperties.getStatusQueueName()).thenReturn("status.queue.test");
    when(statusQueue.subscribe(eq("status.queue.test"), eq(JobStatusUpdateDTO.class), any()))
        .thenReturn(subscription);

    var statusManagerService =
        new StatusManagerService(statusQueue, commonProperties, jobStatusUpdateService);

    statusManagerService.start();
    @SuppressWarnings({"unchecked"})
    ArgumentCaptor<MessageHandler<JobStatusUpdateDTO>> handlerCaptor =
        ArgumentCaptor.forClass(MessageHandler.class);

    verify(statusQueue)
        .subscribe(eq("status.queue.test"), eq(JobStatusUpdateDTO.class), handlerCaptor.capture());

    var handler = handlerCaptor.getValue();
    var ack = mock(Acknowledger.class);

    @SuppressWarnings("unchecked")
    var queueMessage = (Message<JobStatusUpdateDTO>) mock(Message.class);

    when(queueMessage.payload()).thenReturn(null);

    when(jobStatusUpdateService.apply(null))
        .thenThrow(new IllegalJobStatusUpdateException("Status update must not be null."));

    handler.handle(queueMessage, ack);

    verify(jobStatusUpdateService).apply(null);
    verify(ack).nack(false);
    verify(ack, never()).ack();
  }

  @Test
  void testCorrectClosureOfServiceWhenStarted() {
    var statusQueue = mock(QueueClient.class);
    var commonProperties = mock(CommonProperties.class);
    var jobStatusUpdateService = mock(JobStatusUpdateService.class);
    var subscription = mock(Subscription.class);

    when(commonProperties.getStatusQueueName()).thenReturn("status.queue.test");
    when(statusQueue.subscribe(eq("status.queue.test"), eq(JobStatusUpdateDTO.class), any()))
        .thenReturn(subscription);

    var statusManagerService =
        new StatusManagerService(statusQueue, commonProperties, jobStatusUpdateService);

    statusManagerService.start();
    statusManagerService.close();
    verify(subscription).close();
    verify(statusQueue).close();
  }

  @Test
  void testCorrectClosureOfServiceWhenNotStarted() {
    var statusQueue = mock(QueueClient.class);
    var commonProperties = mock(CommonProperties.class);
    var jobStatusUpdateService = mock(JobStatusUpdateService.class);
    var subscription = mock(Subscription.class);
    var statusManagerService =
        new StatusManagerService(statusQueue, commonProperties, jobStatusUpdateService);

    statusManagerService.close();
    verify(subscription, never()).close();
    verify(statusQueue).close();
  }
}

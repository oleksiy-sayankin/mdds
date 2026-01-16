/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import com.google.common.annotations.VisibleForTesting;
import com.mdds.common.CommonProperties;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.TaskDTO;
import com.mdds.dto.TaskStatus;
import com.mdds.grpc.solver.GetTaskStatusRequest;
import com.mdds.grpc.solver.GetTaskStatusResponse;
import com.mdds.grpc.solver.GrpcTaskStatus;
import com.mdds.grpc.solver.RequestStatus;
import com.mdds.grpc.solver.Row;
import com.mdds.grpc.solver.SolverServiceGrpc;
import com.mdds.grpc.solver.SubmitTaskRequest;
import com.mdds.queue.Acknowledger;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import com.mdds.queue.Queue;
import jakarta.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/** Solves system of linear algebraic equations. */
@Slf4j
@Component
public class ExecutorMessageHandler implements MessageHandler<TaskDTO> {
  private final SolverServiceGrpc.SolverServiceBlockingStub solverStub;
  private final Queue resultQueue;
  private final GrpcChannel grpcChannel;
  private final CommonProperties commonProperties;
  private final ExecutorProperties executorProperties;

  @Autowired
  public ExecutorMessageHandler(
      @Qualifier("resultQueue") Queue resultQueue,
      GrpcChannel grpcChannel,
      CommonProperties commonProperties,
      ExecutorProperties executorProperties) {
    this.grpcChannel = grpcChannel;
    this.solverStub = SolverServiceGrpc.newBlockingStub(grpcChannel.getChannel());
    this.resultQueue = resultQueue;
    this.commonProperties = commonProperties;
    this.executorProperties = executorProperties;
    log.info(
        "Created Executor Message Handler '{}', {}, gRPC Server {}:{}",
        commonProperties.getResultQueueName(),
        resultQueue,
        grpcChannel.getHost(),
        grpcChannel.getPort());
  }

  @VisibleForTesting
  public ExecutorMessageHandler(
      Queue resultQueue,
      SolverServiceGrpc.SolverServiceBlockingStub solverStub,
      GrpcChannel channel,
      CommonProperties commonProperties,
      ExecutorProperties executorProperties) {
    this.grpcChannel = channel;
    this.resultQueue = resultQueue;
    this.solverStub = solverStub;
    this.commonProperties = commonProperties;
    this.executorProperties = executorProperties;
  }

  @Override
  public void handle(@Nonnull Message<TaskDTO> message, @Nonnull Acknowledger ack) {
    var payload = message.payload();
    var taskId = payload.getId();
    var taskCreationDateTime = payload.getDateTime();
    log.info("Start handling task {}", taskId);
    var submitTaskRequest = buildSubmitTaskRequest(payload);
    var submitTaskResponse = solverStub.submitTask(submitTaskRequest);
    var submitTaskResponseStatus = submitTaskResponse.getRequestStatus();
    log.info(
        "Got response to submit solve task request. Task id = {}, request status = {}, details ="
            + " {}",
        taskId,
        submitTaskResponseStatus,
        submitTaskResponse.getRequestStatusDetails());

    if (RequestStatus.COMPLETED.equals(submitTaskResponseStatus)) {
      // create message with 'in progress' status
      var inProgress =
          new ResultDTO(
              taskId,
              taskCreationDateTime,
              Instant.now(),
              TaskStatus.IN_PROGRESS,
              executorProperties.getCancelQueueName(),
              30,
              null,
              "");
      log.info("Publishing in-progress status for task {}", taskId);
      publish(new Message<>(inProgress, new HashMap<>(), Instant.now()));
      var getTaskStatusRequest = buildGetTaskStatusRequest(taskId);
      var timeOut = Duration.ofSeconds(600);
      AtomicInteger retryCount = new AtomicInteger(1);
      var validStatuses =
          Set.of(GrpcTaskStatus.DONE, GrpcTaskStatus.CANCELLED, GrpcTaskStatus.ERROR);
      AtomicReference<GetTaskStatusResponse> getTaskStatusResponse = new AtomicReference<>();
      try {
        Awaitility.await()
            .atMost(timeOut)
            .pollInterval(Duration.ofSeconds(1))
            .pollDelay(Duration.ZERO)
            .logging((s -> log.info("Requesting for task status. Retry count {}", retryCount)))
            .ignoreExceptions()
            .until(
                () -> {
                  retryCount.incrementAndGet();
                  getTaskStatusResponse.set(solverStub.getTaskStatus(getTaskStatusRequest));

                  var getTaskStatusResponseStatus = getTaskStatusResponse.get().getRequestStatus();
                  log.info(
                      "Task status response: id {}, status {}, message '{}'",
                      taskId,
                      getTaskStatusResponse.get().getGrpcTaskStatus(),
                      getTaskStatusResponse.get().getTaskMessage());
                  if (RequestStatus.COMPLETED.equals(getTaskStatusResponseStatus)) {
                    return validStatuses.contains(getTaskStatusResponse.get().getGrpcTaskStatus());
                  }
                  log.warn(
                      "Task status request completed with status {} for task id {}",
                      getTaskStatusResponseStatus,
                      taskId);
                  return false;
                });

        var taskResponse = getTaskStatusResponse.get();
        var status = taskResponse.getGrpcTaskStatus();
        var taskMessage = taskResponse.getTaskMessage();
        switch (status) {
          case DONE -> {
            // create message with solution and publish to result queue
            var solution =
                taskResponse.getSolutionList().stream().mapToDouble(Double::doubleValue).toArray();
            var resultArray =
                new ResultDTO(
                    taskId,
                    taskCreationDateTime,
                    Instant.now(),
                    TaskStatus.DONE,
                    executorProperties.getCancelQueueName(),
                    100,
                    solution,
                    taskMessage);
            log.info("Solved system of linear algebraic equations task {}", taskId);
            publish(new Message<>(resultArray, new HashMap<>(), Instant.now()));
          }
          case CANCELLED -> {
            // create cancel message
            var cancelled =
                new ResultDTO(
                    taskId,
                    taskCreationDateTime,
                    Instant.now(),
                    TaskStatus.CANCELLED,
                    executorProperties.getCancelQueueName(),
                    70,
                    new double[] {},
                    taskMessage);
            log.warn("Successfully cancelled task {}", taskId);
            publish(new Message<>(cancelled, new HashMap<>(), Instant.now()));
          }
          case ERROR -> {
            // create error message
            var errorResult =
                new ResultDTO(
                    taskId,
                    taskCreationDateTime,
                    Instant.now(),
                    TaskStatus.ERROR,
                    executorProperties.getCancelQueueName(),
                    70,
                    new double[] {},
                    taskMessage);
            log.error("Internal error for task {}", taskId);
            publish(new Message<>(errorResult, new HashMap<>(), Instant.now()));
          }
          default -> {
            // create error message
            var errorResult =
                new ResultDTO(
                    taskId,
                    taskCreationDateTime,
                    Instant.now(),
                    TaskStatus.ERROR,
                    executorProperties.getCancelQueueName(),
                    70,
                    new double[] {},
                    taskMessage);
            log.error("Unexpected status '{}' for task {}", status, taskId);
            publish(new Message<>(errorResult, new HashMap<>(), Instant.now()));
          }
        }
      } catch (ConditionTimeoutException e) {
        // create error message
        var errorResult =
            new ResultDTO(
                taskId,
                taskCreationDateTime,
                Instant.now(),
                TaskStatus.ERROR,
                executorProperties.getCancelQueueName(),
                70,
                new double[] {},
                "Timeout exception");
        log.error("Timeout exception for task {}", taskId);
        publish(new Message<>(errorResult, new HashMap<>(), Instant.now()));
      }
    } else {
      var errorResult =
          new ResultDTO(
              taskId,
              taskCreationDateTime,
              Instant.now(),
              TaskStatus.ERROR,
              executorProperties.getCancelQueueName(),
              70,
              new double[] {},
              "Error submitting task");
      log.error(
          "Error submitting task {}, details = {}",
          taskId,
          submitTaskResponse.getRequestStatusDetails());
      publish(new Message<>(errorResult, new HashMap<>(), Instant.now()));
    }
    // inform queue that message was processed
    ack.ack();
  }

  private void publish(Message<ResultDTO> resultMessage) {
    resultQueue.publish(commonProperties.getResultQueueName(), resultMessage);
    log.info(
        "Published result for task with id {} and status {} to queue '{}', {}",
        resultMessage.payload().getTaskId(),
        resultMessage.payload().getTaskStatus(),
        commonProperties.getResultQueueName(),
        resultQueue);
  }

  private static GetTaskStatusRequest buildGetTaskStatusRequest(String taskId) {
    log.info("Building get task status request for task {}", taskId);
    var requestBuilder = GetTaskStatusRequest.newBuilder().setTaskId(taskId);
    return requestBuilder.build();
  }

  private static SubmitTaskRequest buildSubmitTaskRequest(TaskDTO payload) {
    log.info("Building submit solve task request for task {}", payload.getId());
    // set solving method for gRPC request
    var requestBuilder =
        SubmitTaskRequest.newBuilder().setMethod(payload.getSlaeSolvingMethod().getName());

    // set matrix for gRPC request
    for (var row : payload.getMatrix()) {
      var rowBuilder = Row.newBuilder();
      for (var value : row) {
        rowBuilder.addValues(value);
      }
      requestBuilder.addMatrix(rowBuilder.build());
    }

    // set right hand side for gRPC request
    for (var value : payload.getRhs()) {
      requestBuilder.addRhs(value);
    }

    // set task id
    requestBuilder.setTaskId(payload.getId());
    return requestBuilder.build();
  }

  @Override
  public String toString() {
    return "ExecutorMessageHandler[gRPC="
        + grpcChannel.getHost()
        + ":"
        + grpcChannel.getPort()
        + ", '"
        + commonProperties.getResultQueueName()
        + "',"
        + resultQueue
        + "]";
  }
}

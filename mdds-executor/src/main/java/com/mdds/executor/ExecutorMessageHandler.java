/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import com.google.common.annotations.VisibleForTesting;
import com.mdds.common.CommonProperties;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.TaskDTO;
import com.mdds.grpc.solver.GetTaskStatusRequest;
import com.mdds.grpc.solver.GetTaskStatusResponse;
import com.mdds.grpc.solver.RequestStatus;
import com.mdds.grpc.solver.Row;
import com.mdds.grpc.solver.SolverServiceGrpc;
import com.mdds.grpc.solver.SubmitTaskRequest;
import com.mdds.grpc.solver.SubmitTaskResponse;
import com.mdds.grpc.solver.TaskStatus;
import com.mdds.queue.Acknowledger;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import com.mdds.queue.Queue;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
  private static final Duration TASK_TIMEOUT = Duration.ofSeconds(600);
  private static final Duration POLL_INTERVAL = Duration.ofSeconds(1);
  private static final Duration POLL_DELAY = Duration.ZERO;
  private static final Duration GRPC_REQUEST_TIMEOUT = Duration.ofSeconds(5);
  private static final Set<TaskStatus> VALID_STATUSES =
      Set.of(TaskStatus.DONE, TaskStatus.CANCELLED, TaskStatus.ERROR);

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
    var task = message.payload();
    try {
      log.info("Start handling task {}", task.getId());
      var submitResponse = submit(task);
      if (!completed(submitResponse)) {
        publishErrorFor(task, "Error submitting task: " + submitResponse.getRequestStatusDetails());
        return;
      }
      publishInProgressFor(task);
      var result = awaitForResultFrom(task);
      publishResponse(result);
    } catch (ConditionTimeoutException e) {
      publishErrorFor(task, "Timeout waiting for task status");
    } catch (StatusRuntimeException e) {
      publishErrorFor(task, "Internal gRPC error: " + e.getStatus());
    } catch (Exception e) {
      publishErrorFor(
          task, "Unexpected error: " + e.getClass().getSimpleName() + ": " + e.getMessage());
    } finally {
      ack.ack();
    }
  }

  private SubmitTaskResponse submit(TaskDTO payload) {
    var submitTaskRequest = buildSubmitTaskRequest(payload);
    return solverStub.withDeadlineAfter(GRPC_REQUEST_TIMEOUT).submitTask(submitTaskRequest);
  }

  private void publishInProgressFor(TaskDTO payload) {
    // create message with 'in progress' status
    var taskId = payload.getId();
    var taskCreationDateTime = payload.getDateTime();
    var inProgress =
        new ResultDTO(
            taskId,
            taskCreationDateTime,
            Instant.now(),
            TaskStatus.IN_PROGRESS,
            executorProperties.getCancelQueueName(),
            30,
            new double[] {},
            "");
    log.info("Publishing in-progress status for task {}", taskId);
    publish(new Message<>(inProgress, Map.of(), Instant.now()));
  }

  private void publishErrorFor(TaskDTO payload, String message) {
    // create error message
    var errorResult =
        new ResultDTO(
            payload.getId(),
            payload.getDateTime(),
            Instant.now(),
            TaskStatus.ERROR,
            executorProperties.getCancelQueueName(),
            70,
            new double[] {},
            message);
    log.error("Error for task {} with message '{}'", payload.getId(), message);
    publish(new Message<>(errorResult, Map.of(), Instant.now()));
  }

  private static Instant toInstant(com.google.protobuf.Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }

  private void publishResponse(GetTaskStatusResponse response) {
    // create message with solution and publish to result queue
    var solution = response.getSolutionList().stream().mapToDouble(Double::doubleValue).toArray();
    var resultArray =
        new ResultDTO(
            response.getTaskId(),
            toInstant(response.getStartTime()),
            toInstant(response.getEndTime()),
            response.getTaskStatus(),
            executorProperties.getCancelQueueName(),
            response.getProgress(),
            solution,
            response.getTaskMessage());
    log.info(
        "Published response for task {} with message '{}'",
        response.getTaskId(),
        response.getTaskMessage());
    publish(new Message<>(resultArray, Map.of(), Instant.now()));
  }

  private GetTaskStatusResponse awaitForResultFrom(TaskDTO task) {
    var request = buildGetTaskStatusRequest(task.getId());
    var attempts = new AtomicInteger(0);
    return Awaitility.await()
        .atMost(TASK_TIMEOUT)
        .pollInterval(POLL_INTERVAL)
        .pollDelay(POLL_DELAY)
        .ignoreExceptionsMatching(
            // Here we ignore "temporary" problems related to gRPC connection.
            // We expect they can be fixed during next iteration of polling.
            throwable ->
                throwable instanceof StatusRuntimeException sre
                    && switch (sre.getStatus().getCode()) {
                      case UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED -> true;
                      default -> false;
                    })
        .until(
            () -> {
              var response =
                  solverStub.withDeadlineAfter(GRPC_REQUEST_TIMEOUT).getTaskStatus(request);
              log.debug(
                  "Task status attempt {}: id {}, requestStatus {}, taskStatus {}, msg '{}'",
                  attempts.incrementAndGet(),
                  task.getId(),
                  response.getRequestStatus(),
                  response.getTaskStatus(),
                  response.getTaskMessage());
              return response;
            },
            response -> completed(response) && taskIsInTerminalState(response));
  }

  private static boolean taskIsInTerminalState(GetTaskStatusResponse response) {
    return VALID_STATUSES.contains(response.getTaskStatus());
  }

  private static boolean completed(SubmitTaskResponse response) {
    return RequestStatus.COMPLETED.equals(response.getRequestStatus());
  }

  private static boolean completed(GetTaskStatusResponse response) {
    return RequestStatus.COMPLETED.equals(response.getRequestStatus());
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

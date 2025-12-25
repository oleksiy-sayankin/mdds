/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.mdds.common.CommonProperties;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.TaskDTO;
import com.mdds.dto.TaskStatus;
import com.mdds.grpc.solver.Row;
import com.mdds.grpc.solver.SolveRequest;
import com.mdds.grpc.solver.SolveResponse;
import com.mdds.grpc.solver.SolverServiceGrpc;
import com.mdds.queue.Acknowledger;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import com.mdds.queue.Queue;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import jakarta.annotation.Nonnull;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/** Solves system of linear algebraic equations. */
@Slf4j
@Component
public class ExecutorMessageHandler implements MessageHandler<TaskDTO>, AutoCloseable {
  private final SolverServiceGrpc.SolverServiceFutureStub solverStub;
  private final ManagedChannel channel;
  private final Queue resultQueue;
  private final ExecutorService threadExecutor = Executors.newFixedThreadPool(2);
  private final GrpcServerProperties grpcServerProperties;
  private final CommonProperties commonProperties;
  private final ExecutorProperties executorProperties;
  private final ConcurrentMap<String, ListenableFuture<SolveResponse>> activeCalls =
      new ConcurrentHashMap<>();

  @Autowired
  public ExecutorMessageHandler(
      @Qualifier("resultQueue") Queue resultQueue,
      GrpcServerProperties grpcServerProperties,
      CommonProperties commonProperties,
      ExecutorProperties executorProperties) {
    this.grpcServerProperties = grpcServerProperties;
    channel = buildGrpcChannel();
    this.solverStub = SolverServiceGrpc.newFutureStub(channel);
    this.resultQueue = resultQueue;
    this.commonProperties = commonProperties;
    this.executorProperties = executorProperties;
    log.info(
        "Created Executor Message Handler '{}', {}, gRPC Server {}:{}",
        commonProperties.getResultQueueName(),
        resultQueue,
        grpcServerProperties.getHost(),
        grpcServerProperties.getPort());
  }

  @VisibleForTesting
  public ExecutorMessageHandler(
      Queue resultQueue,
      SolverServiceGrpc.SolverServiceFutureStub solverStub,
      ManagedChannel channel,
      GrpcServerProperties grpcServerProperties,
      CommonProperties commonProperties,
      ExecutorProperties executorProperties) {
    this.grpcServerProperties = grpcServerProperties;
    this.channel = channel;
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
    publish(inProgressMessage(taskId, taskCreationDateTime));
    log.info("Start processing task {} with executor '{}'", taskId, executorProperties.getId());
    publish(solvedMessage(buildSolveRequest(payload), taskId, taskCreationDateTime));
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

  private Message<ResultDTO> inProgressMessage(String taskId, Instant taskCreationDateTime) {
    // create message with 'in progress' status
    var resultArray =
        new ResultDTO(
            taskId,
            taskCreationDateTime,
            Instant.now(),
            TaskStatus.IN_PROGRESS,
            executorProperties.getCancelQueueName(),
            30,
            null,
            "");
    return new Message<>(resultArray, new HashMap<>(), Instant.now());
  }

  private static SolveRequest buildSolveRequest(TaskDTO payload) {
    log.info("Building solve request for task {}", payload.getId());
    // set solving method for gRPC request
    var requestBuilder =
        SolveRequest.newBuilder().setMethod(payload.getSlaeSolvingMethod().getName());

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
    return requestBuilder.build();
  }

  private Message<ResultDTO> solvedMessage(
      SolveRequest solveRequest, String taskId, Instant taskCreationDateTime) {
    log.info("Processing solve request for task {}", taskId);
    var future = solverStub.solve(solveRequest);
    activeCalls.put(taskId, future);
    try {
      // RPC call
      var response = future.get();
      // extract solution from response
      var solution = response.getSolutionList().stream().mapToDouble(Double::doubleValue).toArray();
      // create message with solution and publish to result queue
      var resultArray =
          new ResultDTO(
              taskId,
              taskCreationDateTime,
              Instant.now(),
              TaskStatus.DONE,
              executorProperties.getCancelQueueName(),
              100,
              solution,
              "");
      log.info("Solved system of linear algebraic equations task {}", taskId);
      return new Message<>(resultArray, new HashMap<>(), Instant.now());
    } catch (StatusRuntimeException | ExecutionException e) {
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
              e.getMessage());
      log.error("gRPC call failed for task {}", taskId, e);
      return new Message<>(errorResult, new HashMap<>(), Instant.now());
    } catch (CancellationException e) {
      var cancelled =
          new ResultDTO(
              taskId,
              taskCreationDateTime,
              Instant.now(),
              TaskStatus.CANCELLED,
              executorProperties.getCancelQueueName(),
              70,
              new double[] {},
              "Cancelled");
      log.warn("gRPC call cancelled for task {}", taskId, e);
      return new Message<>(cancelled, new HashMap<>(), Instant.now());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // create interrupted message
      var errorResult =
          new ResultDTO(
              taskId,
              taskCreationDateTime,
              Instant.now(),
              TaskStatus.CANCELLED,
              executorProperties.getCancelQueueName(),
              70,
              new double[] {},
              e.getMessage());
      log.error("gRPC call interrupted for task {}", taskId, e);
      return new Message<>(errorResult, new HashMap<>(), Instant.now());
    } finally {
      activeCalls.remove(taskId, future);
    }
  }

  public boolean cancelTask(String taskId) {
    var future = activeCalls.get(taskId);
    return future != null && future.cancel(true);
  }

  @Override
  @PreDestroy
  public void close() {
    channel.shutdown();
    threadExecutor.shutdown();
    try {
      if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
        channel.shutdownNow();
      }
      if (!threadExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        threadExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      log.error("Error during shutdown", e);
      Thread.currentThread().interrupt();
    }
    log.info("All gRPC resources stopped cleanly.");
  }

  @Override
  public String toString() {
    return "ExecutorMessageHandler[gRPC="
        + grpcServerProperties.getHost()
        + ":"
        + grpcServerProperties.getPort()
        + ", '"
        + commonProperties.getResultQueueName()
        + "',"
        + resultQueue
        + "]";
  }

  private ManagedChannel buildGrpcChannel() {
    var grpcServerHost = grpcServerProperties.getHost();
    var grpcServerPort = grpcServerProperties.getPort();
    var grpcChannel =
        NettyChannelBuilder.forAddress(grpcServerHost, grpcServerPort)
            .usePlaintext()
            .executor(threadExecutor)
            .offloadExecutor(threadExecutor)
            .build();
    log.info("Created gRPC channel for {}:{}", grpcServerHost, grpcServerPort);
    return grpcChannel;
  }
}

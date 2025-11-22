/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import com.google.common.annotations.VisibleForTesting;
import com.mdds.common.AppConstants;
import com.mdds.common.AppConstantsFactory;
import com.mdds.dto.ResultDTO;
import com.mdds.dto.TaskDTO;
import com.mdds.dto.TaskStatus;
import com.mdds.grpc.solver.Row;
import com.mdds.grpc.solver.SolveRequest;
import com.mdds.grpc.solver.SolverServiceGrpc;
import com.mdds.queue.Acknowledger;
import com.mdds.queue.Message;
import com.mdds.queue.MessageHandler;
import com.mdds.queue.Queue;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/** Solves system of linear algebraic equations. */
@Slf4j
public class ExecutorMessageHandler implements MessageHandler<TaskDTO>, AutoCloseable {
  private final SolverServiceGrpc.SolverServiceBlockingStub solverStub;
  private final ManagedChannel channel;
  private final @Nonnull Queue resultQueue;
  private final ExecutorService threadExecutor = Executors.newFixedThreadPool(2);
  private final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(2);

  public ExecutorMessageHandler(@Nonnull Queue resultQueue) {
    channel = buildGrpcChannel();
    this.solverStub = SolverServiceGrpc.newBlockingStub(channel);
    this.resultQueue = resultQueue;
  }

  @VisibleForTesting
  public ExecutorMessageHandler(
      @Nonnull Queue resultQueue,
      @Nonnull SolverServiceGrpc.SolverServiceBlockingStub solverStub,
      @Nonnull ManagedChannel channel) {
    this.channel = channel;
    this.resultQueue = resultQueue;
    this.solverStub = solverStub;
  }

  @Override
  public void handle(@Nonnull Message<TaskDTO> message, @Nonnull Acknowledger ack) {
    var payload = message.payload();
    var resultMessage =
        processRequest(buildSolveRequest(payload), payload.getId(), payload.getDateTime());
    resultQueue.publish(
        AppConstantsFactory.getString(AppConstants.RESULT_QUEUE_NAME), resultMessage);
    // inform queue that message was processed
    ack.ack();
  }

  private static SolveRequest buildSolveRequest(TaskDTO payload) {
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

  private Message<ResultDTO> processRequest(
      SolveRequest solveRequest, String taskId, Instant taskCreationDateTime) {
    try {
      // RPC call
      var response = solverStub.solve(solveRequest);
      // extract solution from response
      var solution = response.getSolutionList().stream().mapToDouble(Double::doubleValue).toArray();
      // create message with solution and publish to result queue
      var resultArray =
          new ResultDTO(
              taskId, taskCreationDateTime, Instant.now(), TaskStatus.DONE, 100, solution, "");
      log.debug("Solved system of linear algebraic equations task {}", taskId);
      return new Message<>(resultArray, new HashMap<>(), Instant.now());
    } catch (StatusRuntimeException e) {
      // create error message
      var errorResult =
          new ResultDTO(
              taskId,
              taskCreationDateTime,
              Instant.now(),
              TaskStatus.ERROR,
              70,
              new double[] {},
              e.getStatus().getDescription());
      log.error("gRPC call failed for task {}", taskId, e);
      return new Message<>(errorResult, new HashMap<>(), Instant.now());
    }
  }

  @Override
  public void close() {
    channel.shutdown();
    threadExecutor.shutdown();
    try {
      if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
        channel.shutdownNow();
        if (!channel.awaitTermination(1, TimeUnit.SECONDS)) {
          log.error("Channel did not terminate gRPC channel after forced shutdown.");
        }
      }
      if (!threadExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        threadExecutor.shutdownNow();
        if (threadExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
          log.error("Channel did not terminate thread executor after forced shutdown.");
        }
      }
      eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
    } catch (InterruptedException e) {
      log.error("Error during shutdown", e);
      Thread.currentThread().interrupt();
    }
    log.info("All gRPC resources stopped cleanly.");
  }

  private ManagedChannel buildGrpcChannel() {
    var executorConf = ExecutorConfFactory.fromEnvOrDefaultProperties();
    var grpcServerHost = executorConf.grpcServerHost();
    var grpcServerPort = executorConf.grpcServerPort();
    var grpcChannel =
        NettyChannelBuilder.forAddress(grpcServerHost, grpcServerPort)
            .usePlaintext()
            .executor(threadExecutor)
            .offloadExecutor(threadExecutor)
            .channelType(NioSocketChannel.class)
            .eventLoopGroup(eventLoopGroup)
            .build();
    log.info("Created gRPC channel for {}:{}", grpcServerHost, grpcServerPort);
    return grpcChannel;
  }
}

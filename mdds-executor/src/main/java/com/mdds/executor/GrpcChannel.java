/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/***
 * Ths class represents gRPC channel to connect to Python code.
 */
@Slf4j
@Component
public class GrpcChannel implements AutoCloseable {
  private final GrpcServerProperties grpcServerProperties;
  private static final ExecutorService threadExecutor = Executors.newFixedThreadPool(2);
  @Getter private ManagedChannel channel;

  @Autowired
  public GrpcChannel(GrpcServerProperties grpcServerProperties) {
    this.grpcServerProperties = grpcServerProperties;
  }

  public String getHost() {
    return grpcServerProperties.getHost();
  }

  public int getPort() {
    return grpcServerProperties.getPort();
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

  @PostConstruct
  public void buildGrpcChannel() {
    var grpcServerHost = grpcServerProperties.getHost();
    var grpcServerPort = grpcServerProperties.getPort();
    var maxInboundMessageSize = grpcServerProperties.getMaxInboundMessageSize();
    channel =
        NettyChannelBuilder.forAddress(grpcServerHost, grpcServerPort)
            .usePlaintext()
            .executor(threadExecutor)
            .offloadExecutor(threadExecutor)
            .maxInboundMessageSize(maxInboundMessageSize)
            .build();
    log.info("Created gRPC channel for {}:{}", grpcServerHost, grpcServerPort);
  }
}

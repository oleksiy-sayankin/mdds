/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import jakarta.annotation.Nonnull;

/**
 * Base class for Executor configuration.
 *
 * @param executorHost host where Executor runs.
 * @param executorPort port where Executor runs.
 * @param grpcServerHost host where gRPC Server runs.
 * @param grpcServerPort port where gRPC Server runs.
 * @param webappDirLocation web application directory location.
 */
public record ExecutorConf(
    @Nonnull String executorHost,
    int executorPort,
    @Nonnull String grpcServerHost,
    int grpcServerPort,
    @Nonnull String webappDirLocation) {}

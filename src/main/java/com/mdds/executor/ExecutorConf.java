/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import jakarta.annotation.Nonnull;

/**
 * Base class for Executor configuration.
 *
 * @param host host where Executor runs.
 * @param port port where Executor runs.
 * @param webappDirLocation web application directory location.
 */
public record ExecutorConf(
    @Nonnull String host,
    int port,
    @Nonnull String grpcHost,
    int grpcPort,
    @Nonnull String webappDirLocation) {}

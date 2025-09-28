/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import static com.mdds.common.util.CommonHelper.readPropertiesOrEmpty;
import static com.mdds.common.util.ConfigResolution.resolveInt;
import static com.mdds.common.util.ConfigResolution.resolveString;

import jakarta.annotation.Nonnull;
import java.io.File;

/** Factory for creating Executor configurations. */
public class ExecutorConfFactory {
  private ExecutorConfFactory() {}

  private static final String FILE_NAME = "executor.properties";

  public static @Nonnull ExecutorConf fromEnvOrDefaultProperties() {
    var props = readPropertiesOrEmpty(FILE_NAME);
    var host = resolveString("mdds.executor.host", "MDDS_EXECUTOR_HOST", props, "localhost");
    var port = resolveInt("mdds.executor.port", "MDDS_EXECUTOR_PORT", props, 35232);
    var grpcHost =
        resolveString("mdds.executor.grpc.host", "MDDS_EXECUTOR_GRPC_HOST", props, "localhost");
    var grpcPort = resolveInt("mdds.executor.grpc.port", "MDDS_EXECUTOR_GRPC_PORT", props, 50051);
    var webappDirLocation =
        resolveString(
            "mdds.executor.webapp.dir.location",
            "MDDS_EXECUTOR_WEBAPP_DIR_LOCATION",
            props,
            System.getProperty("user.dir") + File.separator + "mdds_executor");
    return new ExecutorConf(host, port, grpcHost, grpcPort, webappDirLocation);
  }
}

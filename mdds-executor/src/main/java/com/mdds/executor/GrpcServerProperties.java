/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Host and port properties of gRPC server. We read it from yml configuration file and inject them
 * while creating {@link com.mdds.executor.ExecutorMessageHandler}.
 */
@ConfigurationProperties(prefix = "mdds.executor.grpc.server")
@Getter
@Setter
public class GrpcServerProperties {
  private String host;
  private int port;
}

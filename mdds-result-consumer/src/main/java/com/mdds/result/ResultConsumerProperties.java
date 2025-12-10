/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.result;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Host and port properties of result consumer as web service. */
@ConfigurationProperties(prefix = "mdds.result.consumer")
@Getter
@Setter
public class ResultConsumerProperties {
  private String host;
  private int port;
}

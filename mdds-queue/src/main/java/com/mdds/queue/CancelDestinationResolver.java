/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class CancelDestinationResolver {
  @Value("${mdds.cancel.destination-prefix:cancel.queue-}")
  private String prefix;

  public String destinationFor(String executorId) {
    return prefix + executorId;
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common.util;

import java.io.IOException;
import java.net.ServerSocket;
import lombok.extern.slf4j.Slf4j;

/** Helper class with common utilities. */
@Slf4j
public final class CommonHelper {

  private CommonHelper() {}

  /**
   * Searches for free port.
   *
   * @return available free port.
   */
  public static int findFreePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new PortException("Failed to find a free port", e);
    }
  }
}

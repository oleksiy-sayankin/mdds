/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Start point for web server. */
@Slf4j
@SpringBootApplication(scanBasePackages = "com.mdds")
public class ServerApplication {
  public static void main(String[] args) {
    SpringApplication.run(ServerApplication.class, args);
  }
}

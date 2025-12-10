/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Health endpoint. Returns status ok. */
@Slf4j
@RestController
public class ExecutorHealthController {
  @GetMapping("/health")
  public String health() {
    return "OK";
  }
}

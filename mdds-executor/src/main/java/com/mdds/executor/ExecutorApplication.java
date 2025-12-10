/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.executor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Gets a task from the Task Queue, solves a task (solves system of a liner equations) and puts
 * result to the Result Queue.
 */
@Slf4j
@SpringBootApplication(scanBasePackages = "com.mdds")
public class ExecutorApplication {
  public static void main(String[] args) {
    SpringApplication.run(ExecutorApplication.class, args);
  }
}

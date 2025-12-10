/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.result;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Gets results from the Result Queue and puts it to storage. */
@Slf4j
@SpringBootApplication(scanBasePackages = "com.mdds")
public class ResultConsumerApplication {
  public static void main(String[] args) {
    SpringApplication.run(ResultConsumerApplication.class, args);
  }
}

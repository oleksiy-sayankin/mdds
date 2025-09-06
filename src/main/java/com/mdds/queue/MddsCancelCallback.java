/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue;

/**
 * Common interface for cancel callback for any types of queue.
 */
public interface MddsCancelCallback {
  void handle();
}

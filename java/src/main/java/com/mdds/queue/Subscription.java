/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue;

/**
 * Subscription for the queue. While subscription object exists, one can work wit the queue. It is
 * expected that one works with queue in this manner: Queue q = QueueHelper.newQueue() // get Queue
 * here and connect to it. Then we do try(Subscription sub = q.subscribe()){ // work with queue here
 * } Here queue is closed and since Subscription is AutoCloseable.
 */
public interface Subscription extends AutoCloseable {
  /** Close queue. Usually includes closing channels and all additional resources of the queue. */
  @Override
  void close();
}

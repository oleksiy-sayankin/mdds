/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.queue;

/**
 * Subscription for the queue. While subscription object exists, one can work with the queue. It is
 * expected that one works with queue in this manner: QueueClient q = QueueHelper.newQueue() // get
 * QueueClient here and connect to queue. Then we do try(Subscription sub = q.subscribe()){ // work
 * with queue here } Here queue is closed and since Subscription is AutoCloseable.
 */
public interface Subscription extends AutoCloseable {
  /** Close queue. Usually includes closing channels and all additional resources of the queue. */
  @Override
  void close();
}

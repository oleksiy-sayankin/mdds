/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mdds.dto.ResultDTO;
import com.mdds.grpc.solver.JobStatus;
import com.mdds.queue.Queue;
import com.mdds.storage.DataStorage;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

class TestServerApplicationCancelController {
  private Queue cancelQueue;
  private DataStorage dataStorage;

  @BeforeEach
  void setUp() {
    cancelQueue = mock(Queue.class);
    dataStorage = mock(DataStorage.class);
  }

  @Test
  void testCancel() {
    var scc = new ServerCancelController(dataStorage, cancelQueue);
    var jobId = "testJobId";
    var result =
        new ResultDTO(
            jobId,
            Instant.now(),
            Instant.now(),
            JobStatus.IN_PROGRESS,
            "cancel.queue",
            10,
            null,
            "");
    when(dataStorage.get(anyString(), any())).thenReturn(Optional.of(result));
    var actual = scc.cancel(jobId);
    assertThat(actual.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    verify(cancelQueue).publish(eq("cancel.queue"), any());
  }

  @Test
  void testCancelNoQueueName() {
    var scc = new ServerCancelController(dataStorage, cancelQueue);
    var jobId = "testJobId";
    when(dataStorage.get(anyString(), any())).thenReturn(Optional.empty());
    assertThatThrownBy(() -> scc.cancel(jobId))
        .isInstanceOf(CanNotCancelJobException.class)
        .hasMessageContaining("Can not cancel job");
  }

  @Test
  void testCancelDoneJob() {
    var scc = new ServerCancelController(dataStorage, cancelQueue);
    var jobId = "testJobId";
    var result =
        new ResultDTO(
            jobId, Instant.now(), Instant.now(), JobStatus.DONE, "cancel.queue", 100, null, "");
    when(dataStorage.get(anyString(), any())).thenReturn(Optional.of(result));
    assertThatThrownBy(() -> scc.cancel(jobId))
        .isInstanceOf(CanNotCancelJobException.class)
        .hasMessageContaining("Job " + jobId + " is already DONE");
  }

  @Test
  void testCancelEmptyQueueName() {
    var scc = new ServerCancelController(dataStorage, cancelQueue);
    var jobId = "testJobId";
    var result =
        new ResultDTO(jobId, Instant.now(), Instant.now(), JobStatus.IN_PROGRESS, "", 10, null, "");
    when(dataStorage.get(anyString(), any())).thenReturn(Optional.of(result));
    assertThatThrownBy(() -> scc.cancel(jobId))
        .isInstanceOf(CanNotCancelJobException.class)
        .hasMessageContaining("Cancel queue name is empty");
  }
}

/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.mdds.common.CommonProperties;
import com.mdds.dto.SolveRequestDTO;
import com.mdds.dto.TaskIdResponseDTO;
import com.mdds.queue.Queue;
import com.mdds.storage.DataStorage;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestServerApplicationSolveController {
  private Queue queue;
  private DataStorage dataStorage;
  private CommonProperties commonProperties;

  @BeforeEach
  void setUp() {
    queue = mock(Queue.class);
    dataStorage = mock(DataStorage.class);
    commonProperties = mock(CommonProperties.class);
  }

  @Test
  void testDoPost() {
    var ssc = new ServerSolveController(dataStorage, queue, commonProperties);
    var request =
        new SolveRequestDTO(
            "http_request",
            "numpy_exact_solver",
            Map.of(
                "matrix",
                List.of(List.of(1.3, 2.2, 3.7), List.of(7.7, 2.1, 9.3), List.of(1.1, 4.8, 2.3)),
                "rhs",
                List.of(1.3, 2.2, 3.7)));
    var actual = ssc.solve(request);
    assertThat(actual).isNotNull();
    assertThat(actual.getId()).isNotEmpty();
  }

  @Test
  void testDoPostNoMatrix() {
    var ssc = new ServerSolveController(dataStorage, queue, commonProperties);
    var request =
        new SolveRequestDTO(
            "http_request", "numpy_exact_solver", Map.of("rhs", List.of(1.3, 2.2, 3.7)));
    assertThatThrownBy(() -> ssc.solve(request))
        .isInstanceOf(EarlyExitException.class)
        .hasMessage("The list of parameters is incomplete. Missing: [matrix]");
  }

  @Test
  void testDoPostNoRhs() {
    var ssc = new ServerSolveController(dataStorage, queue, commonProperties);
    var request =
        new SolveRequestDTO(
            "http_request",
            "numpy_exact_solver",
            Map.of(
                "matrix",
                List.of(List.of(1.3, 2.2, 3.7), List.of(7.7, 2.1, 9.3), List.of(1.1, 4.8, 2.3))));
    assertThatThrownBy(() -> ssc.solve(request))
        .isInstanceOf(EarlyExitException.class)
        .hasMessage("The list of parameters is incomplete. Missing: [rhs]");
  }

  @Test
  void testDoPostNoSolvingMethod() {
    var ssc = new ServerSolveController(dataStorage, queue, commonProperties);
    var request =
        new SolveRequestDTO(
            "http_request",
            null,
            Map.of(
                "matrix",
                List.of(List.of(1.3, 2.2, 3.7), List.of(7.7, 2.1, 9.3), List.of(1.1, 4.8, 2.3)),
                "rhs",
                List.of(1.3, 2.2, 3.7)));
    assertThatThrownBy(() -> ssc.solve(request))
        .isInstanceOf(EarlyExitException.class)
        .hasMessage("Invalid solving method: null");
  }
}

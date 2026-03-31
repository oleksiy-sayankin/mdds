/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.dto;

import static com.mdds.domain.SlaeSolver.PETSC_SOLVER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.mdds.domain.SlaeSolver;
import com.mdds.domain.SolverParsingException;
import org.junit.jupiter.api.Test;

class TestSlaeSolver {
  @Test
  void testIsValid() {
    assertThat(SlaeSolver.isValid(null)).isFalse();
    assertThat(SlaeSolver.isValid("petsc_solver")).isTrue();
    assertThat(SlaeSolver.isValid("wrong_solver")).isFalse();
  }

  @Test
  void testParse() {
    assertThat(SlaeSolver.parse("petsc_solver")).isEqualTo(PETSC_SOLVER);
    assertThatThrownBy(() -> SlaeSolver.parse("wrong_solver"))
        .isInstanceOf(SolverParsingException.class)
        .hasMessageContaining("Can not parse solving method");
  }
}

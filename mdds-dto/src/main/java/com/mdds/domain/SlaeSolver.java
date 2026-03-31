/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.domain;

import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;

/** List of methods that can be used for System of Linear Equation solving. */
@Getter
public enum SlaeSolver {
  NUMPY_EXACT_SOLVER("numpy_exact_solver"),
  NUMPY_LSTSQ_SOLVER("numpy_lstsq_solver"),
  NUMPY_PINV_SOLVER("numpy_pinv_solver"),
  PETSC_SOLVER("petsc_solver"),
  SCIPY_GMRES_SOLVER("scipy_gmres_solver");

  private final String value;

  SlaeSolver(String value) {
    this.value = value;
  }

  public static Set<String> asStringSet() {
    return Arrays.stream(values())
        .map(SlaeSolver::getValue)
        .collect(Collectors.toUnmodifiableSet());
  }

  public static boolean isValid(String method) {
    if (method == null) {
      return false;
    }
    for (var slaeSolver : SlaeSolver.values()) {
      if (slaeSolver.getValue().equals(method)) {
        return true;
      }
    }
    return false;
  }

  public static @Nonnull SlaeSolver parse(@Nonnull String value) {
    return switch (value) {
      case "numpy_exact_solver" -> NUMPY_EXACT_SOLVER;
      case "numpy_lstsq_solver" -> NUMPY_LSTSQ_SOLVER;
      case "numpy_pinv_solver" -> NUMPY_PINV_SOLVER;
      case "petsc_solver" -> PETSC_SOLVER;
      case "scipy_gmres_solver" -> SCIPY_GMRES_SOLVER;
      default -> throw new SolverParsingException("Can not parse solving method: " + value);
    };
  }
}

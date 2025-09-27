/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package dto;

import jakarta.annotation.Nonnull;
import lombok.Getter;

/** List of methods that can be used for System of Linear Equation solving. */
@Getter
public enum SlaeSolver {
  NUMPY_EXACT_SOLVER("numpy_exact_solver"),
  NUNPY_LSTSQ_SOLVER("numpy_lstsq_solver"),
  NUMPY_PINV_SOLVER("numpy_pinv_solver"),
  PETSC_SOLVER("petsc_solver"),
  SCIPY_GMERS_SOLVER("scipy_gmres_solver");

  private final String name;

  SlaeSolver(String name) {
    this.name = name;
  }

  public static boolean isValid(String method) {
    if (method == null) {
      return false;
    }
    for (var slaeSolver : SlaeSolver.values()) {
      if (slaeSolver.getName().equals(method)) {
        return true;
      }
    }
    return false;
  }

  public static @Nonnull SlaeSolver parse(@Nonnull String value) {
    return switch (value) {
      case "numpy_exact_solver" -> NUMPY_EXACT_SOLVER;
      case "numpy_lstsq_solver" -> NUNPY_LSTSQ_SOLVER;
      case "numpy_pinv_solver" -> NUMPY_PINV_SOLVER;
      case "petsc_solver" -> PETSC_SOLVER;
      case "scipy_gmres_solver" -> SCIPY_GMERS_SOLVER;
      default -> throw new SolverParsingException("Can not parse solving method: " + value);
    };
  }

  private static class SolverParsingException extends RuntimeException {
    private SolverParsingException(String message) {
      super(message);
    }
  }
}

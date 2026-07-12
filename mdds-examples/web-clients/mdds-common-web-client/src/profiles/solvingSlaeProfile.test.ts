// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

import { describe, expect, it } from "vitest";
import { JOB_PROFILES } from "@/profiles/jobProfiles";
import { solvingSlaeProfile } from "@/profiles/solvingSlaeProfile";

/**
 * Verifies the built-in SLAE job profile used by the initial wizard flow.
 */
describe("solvingSlaeProfile", () => {
  it("defines the expected solving_slae job profile", () => {
    expect(solvingSlaeProfile.jobType).toBe("solving_slae");

    expect(solvingSlaeProfile.inputSlots).toEqual([
      {
        name: "matrix",
        format: "csv",
        required: true,
        fileName: "matrix.csv",
      },
      {
        name: "rhs",
        format: "csv",
        required: true,
        fileName: "rhs.csv",
      },
    ]);

    expect(solvingSlaeProfile.outputSlots).toEqual([
      {
        name: "solution",
        format: "csv",
        fileName: "solution.csv",
      },
    ]);
  });

  it("defines solvingMethod as a required enum parameter", () => {
    const solvingMethod = solvingSlaeProfile.params.find(
      (param) => param.name === "solvingMethod",
    );

    expect(solvingMethod).toEqual({
      name: "solvingMethod",
      type: "string",
      required: true,
      defaultValue: "numpy_exact_solver",
      enumValues: [
        "numpy_exact_solver",
        "numpy_lstsq_solver",
        "numpy_pinv_solver",
        "petsc_solver",
        "scipy_gmres_solver",
      ],
    });
  });

  it("registers solving_slae in the supported job profiles registry", () => {
    expect(JOB_PROFILES).toContain(solvingSlaeProfile);
  });
});

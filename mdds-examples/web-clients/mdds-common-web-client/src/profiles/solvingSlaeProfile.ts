// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

import type { JobProfile } from "./JobProfile";

/**
 * Built-in profile for the solving_slae job type.
 *
 * Defines matrix and RHS inputs, solver parameters, and solution output
 * for the initial MDDS demo workflow.
 */
export const solvingSlaeProfile: JobProfile = {
  jobType: "solving_slae",
  inputSlots: [
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
  ],
  params: [
    {
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
    },
    {
      name: "tolerance",
      type: "number",
      required: false,
    },
  ],
  outputSlots: [
    {
      name: "solution",
      format: "csv",
      fileName: "solution.csv",
    },
  ],
};

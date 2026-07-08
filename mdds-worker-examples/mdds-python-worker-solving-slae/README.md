<!--
Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
Refer to the LICENSE file in the root directory for full license details.
-->

# MDDS Python Worker Solving SLAE

This module provides an example Python `JobHandler` implementation for the
MDDS Python Worker Runtime.

The handler solves systems of linear algebraic equations:

```text
A × x = b
```

where:

```text
A — coefficient matrix
x — unknown solution vector
b — right-hand side vector
```

The module is intentionally placed outside `mdds-python-worker-runtime`.
The Worker Runtime is a generic execution host, while this package is a
concrete user-level handler implementation for the `solving_slae` job type.

## Purpose

This package demonstrates how a user-provided Python worker handler can be
implemented and packaged separately from the Worker Runtime.

It is intended to be loaded by the Worker Runtime through a configured handler
import path.

```text
mdds_python_worker_solving_slae.handler.SlaeJobHandler
```

This package does not define its own process entry point. The container process
entry point is provided by the Python Worker Runtime:

```text
mdds-worker-runtime
```

## Runtime role

The Python Worker Runtime is responsible for:

```text
- consuming job messages;
- preparing the execution context;
- downloading declared input artifacts;
- invoking the configured JobHandler;
- publishing job status updates;
- uploading declared output artifacts;
- handling handler execution errors, timeouts, and cancellation.
```

This package is responsible only for job-specific SLAE logic:

```text
- validating SLAE input artifacts and solver parameters;
- reading matrix and right-hand-side input files;
- solving the linear system using the selected method;
- writing the declared solution output artifact.
```

## Expected job contract

The handler expects a `solving_slae` job with the following logical artifacts.

Input artifacts:

```text
matrix -> matrix.csv
rhs    -> rhs.csv
```

Output artifacts:

```text
solution -> solution.csv
```

The matrix file contains the coefficient matrix `A`.
The right-hand-side file contains the right-hand-side vector `b`, with exactly one numeric value per CSV row.
The solution file contains the computed solution `x`.

## Supported SLAE solving methods

The MDDS UI and orchestration layer expose several solver options. Their values
are internal solver identifiers used by the MDDS job orchestration layer.

For a simple, square, non-singular and well-conditioned system, all methods are
expected to return approximately the same solution. For ill-conditioned,
singular, overdetermined, or underdetermined systems, different methods may
produce different results because they solve slightly different mathematical
problems.

| Solver identifier    | Python implementation                        | Mathematical meaning                                                                                                                                                                                                                                                                |
|----------------------|----------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `numpy_exact_solver` | `numpy.linalg.solve(A, b)`                   | Direct solver for square, full-rank linear systems. It computes the solution of `A × x = b` assuming that `A` is square and non-singular. This is the most natural choice for a regular square SLAE.                                                                                |
| `numpy_lstsq_solver` | `numpy.linalg.lstsq(A, b, rcond=None)`       | Least-squares solver. It finds a vector `x` that minimizes the residual norm `‖A × x - b‖₂`. This method is useful for overdetermined systems, inconsistent systems, or cases where an approximate least-squares solution is acceptable.                                            |
| `numpy_pinv_solver`  | `numpy.linalg.pinv(A) × b`                   | Pseudoinverse-based solver using the Moore-Penrose pseudoinverse. It can be used for singular, rectangular, underdetermined, or overdetermined systems. In many cases, it returns a minimum-norm least-squares solution.                                                            |
| `petsc_solver`       | PETSc `KSP` solver, default `GMRES`          | Iterative solver based on PETSc Krylov Subspace Methods. In the current implementation, the matrix is converted to CSR sparse format and solved with PETSc KSP. This method is suitable for larger sparse systems and is closer to high-performance scientific computing workflows. |
| `scipy_gmres_solver` | `scipy.sparse.linalg.gmres(A, b, rtol=1e-8)` | Iterative GMRES solver from SciPy. GMRES is a Krylov subspace method commonly used for general non-symmetric linear systems. The solver iterates until convergence and reports an error if convergence is not achieved.                                                             |

In short:

```text
numpy_exact_solver   — direct solution for regular square systems
numpy_lstsq_solver   — least-squares approximation
numpy_pinv_solver    — pseudoinverse / minimum-norm style solution
petsc_solver         — PETSc iterative solver for scientific/HPC-style workloads
scipy_gmres_solver   — SciPy GMRES iterative solver
```

## Package layout

```text
mdds-worker-examples/
└── mdds-python-worker-solving-slae/
    ├── pyproject.toml
    ├── README.md
    ├── LICENSE
    └── src/
        ├── main/
        │   └── python/
        │       └── mdds_python_worker_solving_slae/
        │           ├── __init__.py
        │           └── handler.py
        └── test/
            └── python/
                └── test_slae_job_handler.py
```

## Build

Build the handler package as a wheel:

```bash
cd mdds-worker-examples/mdds-python-worker-solving-slae
python -m build --wheel --outdir target/dist
```

The generated wheel can later be installed into a Docker image derived from the
Python Worker Runtime image.

## Deployment model

This package is not meant to replace the Python Worker Runtime image.

The intended layering is:

```text
python-base
  -> python-worker-runtime
      -> python-worker-solving-slae
```

The Worker Runtime image provides the process entry point and execution
infrastructure. This package provides only the concrete SLAE `JobHandler`.

A later deployment step should install this package into a Worker Runtime image
and configure the handler import path:

```text
mdds_python_worker_solving_slae.handler.SlaeJobHandler
```

## Scope

This example handler does not own RabbitMQ, S3, status publication, workspace
cleanup, process supervision, timeout handling, or cancellation coordination.
Those responsibilities belong to the Python Worker Runtime.

This package owns only the SLAE-specific validation and execution logic.

<!--
Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
Refer to the LICENSE file in the root directory for full license details.
-->

# MDDS Python Worker NumPy SLAE Solver

This module provides an example Python `WorkerHandler` implementation for the MDDS Python Worker Runtime.

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

| Python implementation      | Mathematical meaning                                                                                                                                                                                 |
|----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `numpy.linalg.solve(A, b)` | Direct solver for square, full-rank linear systems. It computes the solution of `A × x = b` assuming that `A` is square and non-singular. This is the most natural choice for a regular square SLAE. |



## Purpose

This package demonstrates how a user-provided Python worker handler can be implemented and packaged separately from the Worker Runtime.

It is intended to be loaded by the Worker Runtime through a configured handler import path.

```text
mdds_python_worker_solving_slae_numpy.handler:SlaeWorkerHandler
```

This package does not define its own executable. The derived Worker Image inherits the MDDS contract executable `/opt/mdds/bin/mdds-worker` and the execution infrastructure from the Python Worker Runtime Common image.


## Expected worker contract

The handler expects a worker manifest with the following logical artifacts.

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

Both input artifacts must be UTF-8 encoded numeric CSV matrix and vector. CSV fields are interpreted as floating-point values in row order.

Invalid UTF-8, malformed CSV, or a non-numeric field causes execution to fail with a message identifying the invalid input slot. No solution output is written in this case.

The solution is written as UTF-8 encoded CSV with one floating-point value per line.


## Package layout

```text
mdds-examples/
└── workers/
    └── mdds-python-worker-solving-slae-numpy/
        ├── pyproject.toml
        ├── README.md
        ├── LICENSE
        └── src/
            ├── main/
            │   └── python/
            │       └── mdds_python_worker_solving_slae_numpy/
            │           ├── __init__.py
            │           └── handler.py
            └── test/
                └── python/
                    └── tests
                        ├── __init__.py
                        └── test_solving_slae_numpy_handler.py
```

## Build

Build the handler package as a wheel:

```bash
cd mdds-examples/workers/mdds-python-worker-solving-slae-numpy
python -m build --wheel --outdir target/dist
```

The Python Worker Runtime Common image provides the MDDS contract executable `/opt/mdds/bin/mdds-worker` and the execution infrastructure.

## Deployment model

This package is not meant to replace the Python Worker Runtime Common image.

The intended layering is:

```text

python-worker-runtime-common
  -> mdds-python-worker-solving-slae-numpy
```

The generated Argo Workflow invokes this executable explicitly and does not depend on a Docker `ENTRYPOINT`. This package provides only the concrete NumPy SLAE WorkerHandler.
The NumPy SLAE Worker Dockerfile installs this package into the Python Worker Runtime Common image and configures its handler import path.

```text
mdds_python_worker_solving_slae_numpy.handler:SlaeWorkerHandler
```

## Scope

This example handler does not own S3, status publication, workspace cleanup, process supervision, timeout handling, or cancellation coordination.

This package owns only NumPy SLAE-specific input validation, solving, and result serialization logic.

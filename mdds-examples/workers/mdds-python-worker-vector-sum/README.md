<!--
Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
Refer to the LICENSE file in the root directory for full license details.
-->

# MDDS Python Worker Vector Sum

This module provides an example Python `WorkerHandler` implementation for the MDDS Python Worker Runtime.

The handler computes the element-wise sum of two vectors and writes it to the logical `solution` output slot.

```text
sum = A + B
```

where:

* A — Vector of elements \[a<sub>1</sub>, a<sub>2</sub>, ..., a<sub>n</sub> \]
* B — Vector of elements \[b<sub>1</sub>, b<sub>2</sub>, ..., b<sub>n</sub> \]
* sum — Vector of elements \[a<sub>1</sub> + b<sub>1</sub>, a<sub>2</sub> + b<sub>2</sub>, ..., a<sub>n</sub> + b<sub>n</sub> \]

## Purpose

This package demonstrates how a user-provided Python worker handler can be implemented and packaged separately from the Worker Runtime.

It is intended to be loaded by the Worker Runtime through a configured handler import path.

```text
mdds_python_worker_vector_sum.handler:VectorSumWorkerHandler
```

This package does not define its own executable. The derived Worker Image inherits the MDDS contract executable `/opt/mdds/bin/mdds-worker` and the execution infrastructure from the Python Worker Runtime Common image.


## Expected worker contract

The handler expects a worker manifest with the following logical artifacts.

Input artifacts:

```text
vector-a
vector-b
```

Output artifacts:

```text
solution
```

* The `vector-a` file contains the elements for vector `A`.
* The `vector-b` file contains the elements for vector `B`.
* The solution file contains the computed sum `A + B`.

Both input artifacts must be UTF-8 encoded numeric CSV vectors containing the same number of elements. CSV fields are interpreted as floating-point values in row order.

Invalid UTF-8, malformed CSV, or a non-numeric field causes execution to fail with a message identifying the invalid input slot. No solution output is written in this case.

The solution is written as UTF-8 encoded CSV with one floating-point value per line.


## Package layout

```text
mdds-examples/
└── workers/
    └── mdds-python-worker-vector-sum/
        ├── pyproject.toml
        ├── README.md
        ├── LICENSE
        └── src/
            ├── main/
            │   └── python/
            │       └── mdds_python_worker_vector_sum/
            │           ├── __init__.py
            │           └── handler.py
            └── test/
                └── python/
                    └── tests
                        ├── __init__.py
                        └── test_vector_sum_handler.py
```

## Build

Build the handler package as a wheel:

```bash
cd mdds-examples/workers/mdds-python-worker-vector-sum
python -m build --wheel --outdir target/dist
```

The Python Worker Runtime Common image provides the MDDS contract executable `/opt/mdds/bin/mdds-worker` and the execution infrastructure.

## Deployment model

This package is not meant to replace the Python Worker Runtime Common image.

The intended layering is:

```text

python-worker-runtime-common
  -> python-worker-vector-sum
```

The generated Argo Workflow invokes this executable explicitly and does not depend on a Docker `ENTRYPOINT`. This package provides only the concrete vector sum `WorkerHandler`.
The Vector Sum Worker Dockerfile installs this package into the Python Worker Runtime Common image and configures its handler import path.

```text
mdds_python_worker_vector_sum.handler:VectorSumWorkerHandler
```

## Scope

This example handler does not own S3, status publication, workspace cleanup, process supervision, timeout handling, or cancellation coordination.

This package owns only the vector sum specific validation and execution logic.

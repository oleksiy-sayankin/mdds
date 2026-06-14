<!--
Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
Refer to the LICENSE file in the root directory for full license details.
-->

# MDDS — Modeling the Dynamics of Distributed Systems

[![Build](https://github.com/oleksiy-sayankin/mdds/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/oleksiy-sayankin/mdds/actions/workflows/build.yml)
[![Coverage](https://codecov.io/gh/oleksiy-sayankin/mdds/branch/master/graph/badge.svg)](https://codecov.io/gh/oleksiy-sayankin/mdds)
![Java](https://img.shields.io/badge/Java-21-blue)
![Maven](https://img.shields.io/badge/Maven-3.9-blue)
![Python](https://img.shields.io/badge/Python-3.12-blue)
![Docker](https://img.shields.io/badge/Docker-29-blue)
![Docker%20Compose](https://img.shields.io/badge/Docker%20Compose-v5.1-blue)
![Status](https://img.shields.io/badge/status-experimental-orange)
![License](https://img.shields.io/badge/License-PolyForm%20Noncommercial%201.0.0-lightgrey)

MDDS is an experimental distributed job orchestration platform.

The project was created as part of dissertation work at the Glushkov Institute of Cybernetics of the National Academy of Sciences of Ukraine.

At the current stage, MDDS demonstrates how a distributed system can accept a computational job, enqueue it, execute it asynchronously, track progress, and return the result to the user.

The currently implemented demo job type is solving a system of linear algebraic equations:

```text
A × x = b
```

## What MDDS does

MDDS is a distributed job orchestration platform for asynchronous and parallel processing of computational tasks.

The system is intended to serve as a reusable execution infrastructure for jobs of different classes. Each job class can define its own input artifacts, parameters, execution strategy, and output artifacts.

The current demo focuses on solving systems of linear algebraic equations, but SLAE solving is only one concrete job profile implemented on top of the general orchestration model.

The platform is responsible for:

- accepting jobs from a web UI or API;
- validating job input metadata;
- storing job metadata;
- publishing jobs to a queue;
- executing jobs asynchronously;
- tracking job status and progress;
- collecting job results;
- exposing the final result to the user.


## Supported SLAE solving methods

The demo UI exposes several solver options. Their values are internal solver identifiers used by the MDDS job orchestration layer.

All solvers are used to solve a system of linear algebraic equations:

```text
A × x = b
```

where:

```text
A — coefficient matrix
x — unknown solution vector
b — right-hand side vector
```

For a simple, square, non-singular and well-conditioned system, all methods are expected to return approximately the same solution. For ill-conditioned, singular, overdetermined, or underdetermined systems, different methods may produce different results because they solve slightly different mathematical problems.

| Solver identifier    | Python implementation                        | Mathematical meaning                                                                                                                                                                                                                                                                |
|----------------------|----------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `numpy_exact_solver` | `numpy.linalg.solve(A, b)`                   | Direct solver for square, full-rank linear systems. It computes the solution of `A × x = b` assuming that `A` is square and non-singular. This is the most natural choice for a regular square SLAE.                                                                                |
| `numpy_lstsq_solver` | `numpy.linalg.lstsq(A, b, rcond=None)`       | Least-squares solver. It finds a vector `x` that minimizes the residual norm `‖A × x - b‖₂`. This method is useful for overdetermined systems, inconsistent systems, or cases where an approximate least-squares solution is acceptable.                                            |
| `numpy_pinv_solver`  | `numpy.linalg.pinv(A) × b`                   | Pseudoinverse-based solver using the Moore–Penrose pseudoinverse. It can be used for singular, rectangular, underdetermined, or overdetermined systems. In many cases, it returns a minimum-norm least-squares solution.                                                            |
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



## Architecture

The demo stack contains:

```text
web-server        — HTTP API and static web UI
executor          — consumes and executes submitted jobs
result-consumer   — consumes execution results and stores them
grpc-server       — numerical computation service
rabbitmq          — job and result queues
redis             — result/progress storage
postgres          — metadata storage
minio             — S3-compatible object storage
loki              — log storage
alloy             — log collector
grafana           — dashboards and log UI
```

For more details see [ARCHITECTURE](./ARCHITECTURE.md).

## Quick start with Docker Compose

### Requirements

MDDS is intended to run on a Linux host with Docker Engine and the Docker Compose plugin.

The primary tested host platform is Ubuntu Linux on `x86_64`/`amd64`.

| Requirement                | Command          | Version policy                                                    |
|----------------------------|------------------|-------------------------------------------------------------------|
| Git                        | `git`            | Any modern Git 2.x version should be sufficient                   |
| Docker Engine              | `docker`         | Must be running and accessible to the current user without `sudo` |
| Docker Compose plugin      | `docker compose` | Must support `docker compose up --wait` and `--wait-timeout`      |
| POSIX-like shell utilities | `sh`, `stat`     | Required for helper commands used by the development workflow     |

The host system does not need Java, Maven, Python, Node.js, npm, or Newman to start the demo stack. These tools are provided by the MDDS development container for the recommended development workflow.

Check installed versions:

```bash
git --version
docker --version
docker compose version
docker compose up --help | grep -- --wait
```

For Docker installation instructions, use the [official Docker documentation for Ubuntu](https://docs.docker.com/engine/install/ubuntu/).

### Configure Docker access on Ubuntu Linux

The Docker CLI must be able to communicate with Docker Engine. First, verify that the Docker daemon is running and accessible to the current user:

```bash
docker info
```

If the command completes successfully and displays both the `Client` and `Server` sections, no additional Docker access configuration is required.

If it reports that it cannot connect to the Docker daemon:

```text
Cannot connect to the Docker daemon
```

start Docker Engine:

```bash
sudo systemctl enable --now docker
docker info
```

If it reports a permission error for `/var/run/docker.sock`:

```text
permission denied while trying to connect to the docker API at unix:///var/run/docker.sock
```

configure access for the current user once:

```bash
getent group docker >/dev/null || sudo groupadd docker
sudo usermod -aG docker "$USER"
```

Apply the updated group membership by logging out of the Ubuntu user session completely and logging in again.

Closing and reopening only the terminal window may not be sufficient because a new terminal can inherit the group membership of the existing graphical login session.

After logging in again, verify that the `docker` group is active:

```bash
id -nG
```

The output must include the `docker` group.

Alternatively, activate the group immediately without logging out:

```bash
if ! command -v newgrp >/dev/null; then
  sudo apt update
  sudo apt install -y util-linux-extra
fi

newgrp docker
```

The `newgrp docker` command starts a new shell with the `docker` group active.

Verify Docker access:

```bash
docker info
docker run --rm hello-world
```

Both commands must complete successfully without `sudo`.

Run `exit` to leave this shell when it is no longer needed.

> **Security note:** membership in the `docker` group grants privileges comparable to root access. Add only trusted users to this group.

### Required free ports

The demo stack publishes several local ports, including:

```text
8000   — MDDS web UI
3000   — Grafana
3100   — Loki
12345  — Grafana Alloy
```

If the stack fails to start, check that the ports defined in `mdds-demo/compose.demo.yml` are free.

### Recommended system resources

| Resource | Requirement               |
|----------|---------------------------|
| CPU      | 4 cores recommended       |
| RAM      | 8 GB recommended          |
| Disk     | 10–20 GB free recommended |



### Tested host environment

The current local workflow has been tested on the following host environment:

| Component             | Tested version        |
|-----------------------|-----------------------|
| Host OS               | Ubuntu 26.04 LTS      |
| Docker Engine         | `29.5.2` and `29.5.3` |
| Docker Compose plugin | `v5.1.4`              |
| Git                   | `2.53.0`              |

### 1. Clone repository

```bash
git clone https://github.com/oleksiy-sayankin/mdds.git
cd mdds
```

### 2. Start demo stack

The quick start expects the MDDS application images under the `mddsproject/*:0.1.0` namespace to be available either locally or in the Docker registry:

```text
mddsproject/grpc-server:0.1.0
mddsproject/executor:0.1.0
mddsproject/web-server:0.1.0
mddsproject/result-consumer:0.1.0
```

Local source-based image builds are covered in the Development section.

```bash
docker compose -f mdds-demo/compose.demo.yml up -d --build --wait --wait-timeout 120
```

This command pulls or uses the prebuilt MDDS application images, builds the local observability images required by the demo stack, starts all services, and waits until services with health checks become healthy.

### 3. Check that services are running

```bash
docker compose -f mdds-demo/compose.demo.yml ps
```

### 4. Open the web UI

Open in browser:

```text
http://localhost:8000
```

### 5. Create sample input files or use files from `mdds-demo/sample-data`

Create `matrix.csv`:

```csv
3,2
1,4
```

Create `rhs.csv`:

```csv
10
8
```

### 6. Solve the system

In the web UI:

1. Upload `matrix.csv`.
2. Upload `rhs.csv`.
3. Select solving method.
4. Click `Solve`.
5. Click `Download solution`.

The expected solution vector is approximately:

```text
x = [2.4, 1.4]
```

### 7. Observability

Grafana is available at:

```text
http://localhost:3000
```

Useful endpoints:

```text
http://localhost:3100/ready
http://localhost:12345
```

You can also inspect logs from the command line:

```bash
docker compose -f mdds-demo/compose.demo.yml logs -f --tail=100 loki alloy grafana
```

### 8. Stop demo stack

```bash
docker compose -f mdds-demo/compose.demo.yml down
```

To remove volumes as well:

```bash
docker compose -f mdds-demo/compose.demo.yml down -v
```

## Development

MDDS local development is based on a Docker-based development container.

This section assumes that the host requirements listed in the Quick start section are already installed.

The development container provides the project toolchain required for building, formatting, testing, and running MDDS from source. The host system is used only to run Docker and to store the project checkout.

### Development environment model

| Layer                          | Responsibility                                                                                                       |
|--------------------------------|----------------------------------------------------------------------------------------------------------------------|
| Host system                    | Stores the Git checkout and runs the Docker daemon                                                                   |
| Development container          | Provides the MDDS build and test toolchain                                                                           |
| Demo/e2e Docker Compose stacks | Run MDDS runtime services such as web-server, executor, RabbitMQ, Redis, PostgreSQL, MinIO, Loki, Alloy, and Grafana |

Docker commands executed inside the development container use the host Docker daemon through the mounted Docker socket.

### Toolchain inside the development container

The exact tool versions are defined by the development container image.

| Tool group               | Tools                                                            |
|--------------------------|------------------------------------------------------------------|
| Java build tools         | Java, Maven                                                      |
| Python tools             | Python, pip, pytest, ruff, pylint, pycodestyle, black            |
| Web tools                | Node.js, npm                                                     |
| Docker tools             | Docker CLI, Docker Compose plugin                                |
| Build automation         | Make                                                             |
| Shell/code quality tools | shellcheck, shfmt, google-java-format, xmllint and related tools |
| Test tools               | Newman, pytest, Maven Surefire/Failsafe and related tools        |


### Tested development container toolchain

The current MDDS development container has been tested with the following tool versions:

| Tool                  | Tested version                   |
|-----------------------|----------------------------------|
| Java                  | OpenJDK Temurin `21.0.11+10-LTS` |
| Maven                 | Apache Maven `3.9.12`            |
| Python                | `3.12.13`                        |
| Node.js               | `v22.22.3`                       |
| npm                   | `10.9.8`                         |
| Docker CLI            | `29.5.3`                         |
| Docker Compose plugin | `v5.1.4`                         |
| Make                  | `4.4.1`                          |

### Test Coverage

[![Codecov coverage sunburst](https://codecov.io/github/oleksiy-sayankin/mdds/graphs/sunburst.svg)](https://app.codecov.io/gh/oleksiy-sayankin/mdds)

The diagram above is a Codecov **coverage sunburst**. It provides a visual overview of how automated tests cover different parts of the MDDS source tree.

Each sector represents a directory or source file:

- sectors closer to the center represent higher-level directories;
- outer sectors represent nested directories and individual files;
- the size of a sector reflects the amount of tracked source code in that part of the project, not its coverage percentage;
- green sectors indicate higher test coverage;
- yellow sectors indicate partial or moderate coverage;
- red sectors indicate areas with low test coverage.

Large red or yellow sectors therefore identify substantial parts of the codebase where additional tests may be most valuable. Small sectors represent relatively small files or directories, regardless of their color.

The image is updated from the coverage reports produced by the CI pipeline. Click the diagram to open the interactive Codecov view, where directories and files can be explored in greater detail.

### 1. Clone repository on the host

```bash
git clone https://github.com/oleksiy-sayankin/mdds.git
cd mdds
```

### 2. Start the MDDS development container

Start the development container directly with Docker Compose:

```bash
DOCKER_GID="$(stat -c '%g' /var/run/docker.sock)" \
docker compose -f mdds-dev/compose.dev.yml up -d
```

The `DOCKER_GID` value is passed so that the development container can access the host Docker socket when Docker commands are executed from inside the container.

### 3. Enter the development container

```bash
docker exec -it mdds-dev-shell bash
```

After this step, build and test commands are executed inside the development container.

### 4. Build MDDS images

```bash
make build_release_images
```

This target builds all MDDS release images from the current source tree, including base, application, and observability images.

### 5. Start the demo stack

```bash
make start_mdds_demo
```

This target starts the demo stack from `mdds-demo/compose.demo.yml` and waits until services with health checks become healthy.

### 6. Open the demo UI from the host browser

```text
http://localhost:8000
```

Grafana is available at:

```text
http://localhost:3000
```

### 7. Run tests

```bash
make test_all
```

### 8. Stop the demo stack

```bash
make stop_mdds_demo
```

### 9. Stop the development container

From the host shell:

```bash
docker compose -f mdds-dev/compose.dev.yml down
```

### Optional: reset the demo stack

If Docker containers or volumes were left from an interrupted run, reset the demo stack from the host or from inside the development container:

```bash
docker compose -f mdds-demo/compose.demo.yml down --volumes --remove-orphans
```


## License

This project is licensed under a dual-license model:

- Free for personal, academic, and experimental use under the
  [PolyForm Noncommercial License 1.0.0](https://polyformproject.org/licenses/noncommercial/1.0.0/).

- Commercial or production use is not permitted under the non-commercial license.

- Commercial use is available via a separate license agreement.

Please contact Oleksiy Oleksandrovych Sayankin for commercial licensing details.

See the full license text in the [LICENSE](./LICENSE) file.

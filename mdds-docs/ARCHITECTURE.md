<!-- 
Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
Refer to the LICENSE file in the root directory for full license details.
-->


This document is an initial high-level architecture draft. It captures the current general direction of MDDS and may be refined by subsequent architecture and implementation work.

## Purpose and Architectural Approach

MDDS is designed to simplify the creation and execution of distributed computational workflows whose operations exchange data through explicitly defined inputs and outputs.

A user defines reusable atomic computational operations, describes their input slots, parameters, and output slots, and connects those operations into a directed acyclic graph. An input of a graph node may reference either an external data artifact or an output produced by another node in the same graph.

MDDS acts as a higher-level control and modeling layer above Argo Workflows. It allows users to define data-dependent computational graphs without working directly with Argo Workflow specifications or Kubernetes resources.

Before execution, MDDS validates the graph, checks its data-flow connections, freezes the configuration of the particular DAG Run, and compiles it into an Argo Workflow specification. The resulting workflow is submitted to Argo Server through its REST API.

Argo Workflows and Kubernetes provide the underlying execution infrastructure. They schedule graph nodes, run OCI container images, execute independent nodes in parallel, enforce dependencies, retry failed attempts, track execution state, and stop running workloads when requested.

This separation of responsibilities allows MDDS to focus on the structure and semantics of distributed computations, while Argo Workflows and Kubernetes handle their reliable container-based execution.

### Complete DAG Configuration Example

This example defines a DAG that solves two systems of linear algebraic equations independently and then calculates the element-wise sum of the resulting solution vectors.

The user creates:

* Worker Profiles;
* Worker Implementations;
* a DAG Definition.

MDDS creates the DAG Run when the user starts the DAG. The DAG Run is an immutable snapshot of the concrete execution configuration.

#### Data sources and storages

* DataSource — user write, trusted platform read for input staging;
* RunArtifactStorage — trusted platform read/write;
* ResultStorage — trusted platform write, user read.

Stored in files:

Example of `data-sources.yaml`

```yaml
apiVersion: mdds/v1
kind: DataSources
dataSources:
  - id: default-inputs
    type: s3
    bucket: mdds-inputs
    accessMode: read-only-for-execution
    credentialSecretRef: ...
```

Example of `run-artifact-storages.yaml`

```yaml
apiVersion: mdds/v1
kind: RunArtifactStorages
runArtifactStorages:
  - id: internal-runs
    type: s3
    bucket: mdds-runs
    accessMode: platform-read-write
    credentialSecretRef: ...
```

Example of `result-storages.yaml`
```yaml
apiVersion: mdds/v1
kind: ResultStorages
resultStorages:
  - id: default-results
    type: s3
    bucket: mdds-results
    accessMode: platform-write-user-read
    credentialSecretRef: ...
```

#### Worker Profile

A Worker Profile is a language-independent contract for an atomic computational operation. It defines the input slots, output slots, and parameters that compatible Worker Implementations must support. It does not represent a running Worker Process.

Example of `worker-profiles.yaml`:

```yaml
apiVersion: mdds/v1
kind: WorkerProfiles
worker-profiles:
  # Solves a system of linear algebraic equations.
  - id: solving-slae
    inputs:
      - name: matrix
        artifactType: numeric-matrix
        format: csv
      - name: rhs
        artifactType: numeric-vector
        format: csv
    params:
      - name: tolerance
        type: number
        required: false
    outputs:
      - name: solution
        artifactType: numeric-vector
        format: csv

  # Calculates the element-wise sum of two vectors.
  - id: vector-sum
    inputs:
      - name: vector-a
        artifactType: numeric-vector
        format: csv
      - name: vector-b
        artifactType: numeric-vector
        format: csv
    outputs:
      - name: solution
        artifactType: numeric-vector
        format: csv
```

#### Worker Implementation

A Worker Implementation connects a Worker Profile to a concrete OCI image. The image must implement the operation described by the Worker Profile and satisfy the Atomic Worker Image Contract.

Example of `worker-implementations.yaml`:

```yaml
apiVersion: mdds/v1
kind: WorkerImplementations
worker-implementations:
  - id: solving-slae-python
    workerProfileId: solving-slae
    ociImageReference: mddsproject/python-worker-solving-slae-numpy-exact-solver@sha256:<sha256-digest>

  - id: vector-sum-python
    workerProfileId: vector-sum
    ociImageReference: mddsproject/python-worker-vector-sum@sha256:<sha256-digest>
```

#### DAG Node

A DAG Node is a configured use of a Worker Profile and a Worker Implementation within a DAG Definition.

A DAG Node is a logical entity. It is not bound to a particular Kubernetes Pod. During execution, a DAG Node is compiled into an Argo DAG task, and one or more Worker Pods may execute its attempts.

#### DAG Definition

A DAG Definition is an editable logical description of a computational graph. It declares DAG inputs, DAG nodes, data-flow bindings between nodes, node parameters, and DAG outputs.

Example of `dags.yaml`:

```yaml
apiVersion: mdds/v1
kind: Dags
dags:
  - dagId: example-dag

    #
    # DAG inputs
    #
    inputs:
      matrix-a:
        artifactType: numeric-matrix
        format: csv
      rhs-a:
        artifactType: numeric-vector
        format: csv
      matrix-b:
        artifactType: numeric-matrix
        format: csv
      rhs-b:
        artifactType: numeric-vector
        format: csv

    #
    # DAG nodes
    #
    nodes:
      - nodeId: solve-a
        workerProfileId: solving-slae
        workerImplementationId: solving-slae-python

        inputBindings:
          matrix:
            from:
              dagInput: matrix-a
          rhs:
            from:
              dagInput: rhs-a

        params: {}

      - nodeId: solve-b
        workerProfileId: solving-slae
        workerImplementationId: solving-slae-python

        inputBindings:
          matrix:
            from:
              dagInput: matrix-b
          rhs:
            from:
              dagInput: rhs-b

        params: {}

      - nodeId: sum-a-b
        workerProfileId: vector-sum
        workerImplementationId: vector-sum-python

        inputBindings:
          vector-a:
            from:
              nodeOutput:
                nodeId: solve-a
                outputSlot: solution

          vector-b:
            from:
              nodeOutput:
                nodeId: solve-b
                outputSlot: solution

    #
    # DAG outputs
    #
    outputs:
      final-solution:
        from:
          nodeOutput:
            nodeId: sum-a-b
            outputSlot: solution
```

The data-flow bindings imply the following execution dependencies:

```mermaid
flowchart LR
    SOLVE_A["solve-a"]
    SOLVE_B["solve-b"]
    SUM["sum-a-b"]
    FINAL["final-solution"]

    SOLVE_A --> SUM
    SOLVE_B --> SUM
    SUM --> FINAL
```


The `solve-a` and `solve-b` nodes do not depend on each other and may therefore execute in parallel. The `sum-a-b` node may start only after both solution vectors have been produced successfully.

#### DAG Run

A DAG Run represents one concrete execution of a DAG Definition.

The DAG Run is created by MDDS when the user starts the DAG. It freezes:

* the DAG structure;
* concrete input artifacts;
* Worker Profile definitions;
* Worker Implementation definitions;
* OCI image digests;
* node parameters;
* internal run artifact storage;
* final output destinations.

Subsequent modifications to the original Worker Profiles, Worker Implementations, or DAG Definition do not affect an existing DAG Run.

Example of `dag-runs.yaml`:

```yaml
apiVersion: mdds/v1
kind: DagRuns
dag-runs:
  - dagRunId: run-123
    userId: 12345
    dagId: example-dag

    #
    # Internal storage for attempt-specific and intermediate artifacts
    #
    runArtifactStorage:
      id: internal-runs
      objectKeyPrefix: dag-runs/run-123

    #
    # Concrete DAG input artifacts
    #
    inputs:
      matrix-a:
        dataSourceId: default-inputs
        objectKey: my-data/matrix-a.csv
        artifactType: numeric-matrix
        format: csv

      rhs-a:
        dataSourceId: default-inputs
        objectKey: my-data/rhs-a.csv
        artifactType: numeric-vector
        format: csv

      matrix-b:
        dataSourceId: default-inputs
        objectKey: my-data/matrix-b.csv
        artifactType: numeric-matrix
        format: csv

      rhs-b:
        dataSourceId: default-inputs
        objectKey: my-data/rhs-b.csv
        artifactType: numeric-vector
        format: csv

    #
    # Frozen node configurations
    #
    nodes:
      #
      # Solves the first system.
      #
      - nodeId: solve-a

        workerProfile:
          id: solving-slae
          inputs:
            - name: matrix
              artifactType: numeric-matrix
              format: csv
            - name: rhs
              artifactType: numeric-vector
              format: csv
          params:
            - name: tolerance
              type: number
              required: false
          outputs:
            - name: solution
              artifactType: numeric-vector
              format: csv

        workerImplementation:
          id: solving-slae-python
          workerProfileId: solving-slae
          ociImageReference: mddsproject/python-worker-solving-slae-numpy-exact-solver@sha256:<sha256-digest>

        inputBindings:
          matrix:
            from:
              dagInput: matrix-a
          rhs:
            from:
              dagInput: rhs-a

        params: {}

      #
      # Solves the second system.
      #
      - nodeId: solve-b

        workerProfile:
          id: solving-slae
          inputs:
            - name: matrix
              artifactType: numeric-matrix
              format: csv
            - name: rhs
              artifactType: numeric-vector
              format: csv
          params:
            - name: tolerance
              type: number
              required: false
          outputs:
            - name: solution
              artifactType: numeric-vector
              format: csv

        workerImplementation:
          id: solving-slae-python
          workerProfileId: solving-slae
          ociImageReference: mddsproject/python-worker-solving-slae-numpy-exact-solver@sha256:<sha256-digest>

        inputBindings:
          matrix:
            from:
              dagInput: matrix-b
          rhs:
            from:
              dagInput: rhs-b

        params: {}

      #
      # Calculates the element-wise sum of the two solution vectors.
      #
      - nodeId: sum-a-b

        workerProfile:
          id: vector-sum
          inputs:
            - name: vector-a
              artifactType: numeric-vector
              format: csv
            - name: vector-b
              artifactType: numeric-vector
              format: csv
          outputs:
            - name: solution
              artifactType: numeric-vector
              format: csv

        workerImplementation:
          id: vector-sum-python
          workerProfileId: vector-sum
          ociImageReference: mddsproject/python-worker-vector-sum@sha256:<sha256-digest>

        inputBindings:
          vector-a:
            from:
              nodeOutput:
                nodeId: solve-a
                outputSlot: solution

          vector-b:
            from:
              nodeOutput:
                nodeId: solve-b
                outputSlot: solution

    #
    # Concrete DAG output destinations
    #
    outputs:
      final-solution:
        from:
          nodeOutput:
            nodeId: sum-a-b
            outputSlot: solution
            format: csv

        destination:
          resultStorageId: default-results
          objectKey: my-results/sum-a-b.csv
```

MDDS generates attempt-specific intermediate output locations under the configured run artifact prefix. For example:

```text
dag-runs/run-123/nodes/solve-a/attempts/{attemptId}/outputs/solution
dag-runs/run-123/nodes/solve-b/attempts/{attemptId}/outputs/solution
dag-runs/run-123/nodes/sum-a-b/attempts/{attemptId}/outputs/solution
```

These internal locations are not selected by the user.

After all computational DAG nodes complete successfully, the system-generated `publish-results` Argo DAG task publishes all declared DAG outputs from RunArtifactStorage to their configured ResultStorage destinations. The Argo Workflow succeeds only after `publish-results` completes successfully.


## Atomic Worker Image Contract

An OCI image is considered compatible with MDDS when it satisfies the requirements defined by the Atomic Worker Image Contract.

### Execution Model

Each Worker Pod executes exactly one atomic DAG node attempt.

The worker must run as a finite process. It must perform the assigned operation and terminate. A compatible worker must not behave as a persistent service, message consumer, or background daemon.

### Entrypoint

The image must contain an executable at the following absolute path:

```text
/opt/mdds/bin/mdds-worker
```

MDDS invokes the executable using the following command:

```text
/opt/mdds/bin/mdds-worker run \
    --manifest /opt/mdds/config/worker-manifest.json
```

The executable may be implemented in any programming language. It may be a native binary, an interpreter launcher, or a wrapper around a language-specific runtime.

The Argo Workflow specification explicitly defines this command and does not depend on an image-specific entrypoint.

### Python Worker Runtime Configuration

The Worker Image defines `MDDS_WORKER_NAME`, `MDDS_WORKER_VERSION`, and `MDDS_WORKER_HANDLER`. The generated Argo Workflow supplies `MDDS_ARGO_RETRY_INDEX` for each concrete attempt.

| Variable Name           | Required | Default Value | Meaning                                                                                                                | Example                                              |
|-------------------------|---------:|--------------:|------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|
| `MDDS_WORKER_NAME`      |      Yes |             — | Stable name of the concrete Worker packaged in the OCI image. Used in logs and execution diagnostics.                  | `mdds-python-worker-solving-slae-numpy-exact-solver` |
| `MDDS_WORKER_VERSION`   |      Yes |             — | Version of the concrete Worker packaged in the OCI image. Used in logs and execution diagnostics.                      | `0.1.0`                                              |
| `MDDS_WORKER_HANDLER`   |      Yes |             — | Python import path of the concrete worker handler.                                                                     | `mdds_slae_worker.handler:SlaeWorkerHandler`         |
| `MDDS_ARGO_RETRY_INDEX` |      Yes |             — | Zero-based index of the current Argo-managed attempt: `0` for the initial attempt, `1` for the first retry, and so on. | `0`                                                  |


### Manifest

A Node Run is the execution of one DAG Node within one DAG Run and is identified by `dagRunId` and `nodeId`. Argo may create one or more Node Attempts for a Node Run. A Node Attempt is identified by `dagRunId`, `nodeId`, and the Argo retry index.
The Worker Manifest describes one Node Run. It remains unchanged between attempts. The Worker Runtime derives the current Node Attempt identifier from `MDDS_ARGO_RETRY_INDEX`, for example `attempt-0`, `attempt-1`, and so on.
This is `worker-manifest.json` example for worker profile with `id` = `solving-slae` for `nodeId` = `solve-a`:

```json
{
  "apiVersion": "mdds/v1",
  "kind": "WorkerManifest",
  "execution": {
    "userId": 12345,
    "dagRunId": "<dag-run-id>",
    "nodeId": "solve-a"
  },
  "inputs": {
    "matrix": {
      "path": "/opt/mdds/inputs/matrix",
      "format": "csv"
    },
    "rhs": {
      "path": "/opt/mdds/inputs/rhs",
      "format": "csv"
    }
  },
  "params": {},
  "outputs": {
    "solution": {
      "path": "/opt/mdds/outputs/solution",
      "format": "csv"
    }
  }
}
```

This is `worker-manifest.json` example for worker profile with `id` = `vector-sum` for `nodeId` = `sum-a-b`:

```json
{
  "apiVersion": "mdds/v1",
  "kind": "WorkerManifest",
  "execution": {
    "userId": 12345,
    "dagRunId": "<dag-run-id>",
    "nodeId": "sum-a-b"
  },
  "inputs": {
    "vector-a": {
      "path": "/opt/mdds/inputs/vector-a",
      "format": "csv"
    },
    "vector-b": {
      "path": "/opt/mdds/inputs/vector-b",
      "format": "csv"
    }
  },
  "params": {},
  "outputs": {
    "solution": {
      "path": "/opt/mdds/outputs/solution",
      "format": "csv"
    }
  }
}
```

### Filesystem Layout

The worker must use the following filesystem layout:

```text
/opt/mdds/config/   Worker configuration and worker manifest
/opt/mdds/inputs/   Input artifacts prepared by Argo
/opt/mdds/outputs/  Output artifacts produced by the worker
/opt/mdds/result/   Structured execution result
/opt/mdds/tmp/      Temporary working data
```

Input and output slots are mapped to filesystem paths by slot name:

```text
/opt/mdds/inputs/{inputSlotName}
/opt/mdds/outputs/{outputSlotName}
```

The exact paths and artifact metadata are also provided in the Worker Manifest.

### Input Artifacts

Argo Workflows retrieves input artifacts from the configured artifact repository and places them at the paths declared in the generated Workflow specification.

The worker must read input data only from the paths defined in the Worker Manifest. The worker must not interact directly with S3 or any other object storage. Argo artifact containers and trusted MDDS platform components perform all object-storage transfers.

### Parameters

Worker parameters are provided in:

```text
/opt/mdds/config/worker-manifest.json
```

The worker must not depend on language-specific parameter serialization or environment-variable naming conventions.

### Output Artifacts

The worker must write each declared output artifact to its corresponding path under:

```text
/opt/mdds/outputs/
```

Before reporting successful execution, the worker must:

1. finish all output writes;
2. close and flush all output files;
3. verify that all required output slots exist;
4. ensure that no required output contains an incomplete temporary result.

Temporary output files should be written under `/opt/mdds/tmp/` or with a temporary name and atomically moved to the declared output path after successful completion.

Argo Workflows collects the declared output artifacts after the worker process terminates.

### Exit Status

The worker communicates its primary execution result through the exit status of the main container process.

```text
exit 0 — success
exit 1 — execution failure; no retry by default
exit 2 — contract violation; never retry
Argo Error / transient infrastructure failure — retry according to platform policy
```

MDDS v1 assigns the following conventional meanings:

```text
0  SUCCESS
1  EXECUTION_FAILED
2  WORKER_CONTRACT_VIOLATION
```

Any other non-zero exit code is also treated as failed execution.

Exit code `0` is valid only when all required outputs have been created successfully.

The final authoritative node state is the state reported by Argo Workflows. A worker exit code of `0` does not make the node successful when Argo cannot collect or publish its required output artifacts.

### Diagnostics

The worker must write normal operational logs to standard output and standard error.

When possible, the worker should write a short final diagnostic message to:

```text
/dev/termination-log
```

The worker should also create:

```text
/opt/mdds/result/result.json
```

with structured success or error details.

The result file is diagnostic metadata and is not the authoritative execution state. It may be absent when the container cannot start or is terminated by the infrastructure.

### Termination

The worker must handle process termination correctly.

When it receives `SIGTERM`, the worker must:

1. stop or terminate the worker-specific computation;
2. forward the signal to any supervised child process;
3. perform bounded cleanup;
4. avoid publishing incomplete outputs as successful outputs;
5. terminate before the configured Kubernetes termination grace period expires.

User-requested cancellation is determined by Argo Workflow state and is not represented by a special worker exit code.

### Language-Specific Runtime APIs

The Atomic Worker Image Contract is language-independent and mandatory.

Language-specific runtime APIs are optional convenience layers built on top of this contract. For example, the Python Worker Runtime may preserve the following interface:

```python
class WorkerHandler:
    def execute(self, context: WorkerExecutionContext) -> None:
        ...
```

The runtime is responsible for reading the Worker Manifest, preparing `WorkerExecutionContext`, invoking the handler, validating outputs, writing diagnostic information, handling termination signals, and converting the result into the appropriate process exit code.

A user may implement the Atomic Worker Image Contract directly without using a language-specific MDDS runtime.

### Worker handler data access pattern

A concrete `WorkerHandler` must access worker inputs, parameters, and outputs only through `WorkerExecutionContext`.

The handler must use logical input slots declared in `manifest.inputs` to read input artifacts:

```python
input_bytes = context.inputs.read("inputSlot")
```

For large input artifacts, or when a library expects a file path, the handler should use the local input path instead of loading the full artifact into memory:

```python
input_path = context.inputs.path("inputSlot")
```

If artifact metadata is required, the handler may access it through:

```python
input_artifact = context.inputs.get("inputSlot")
input_format = input_artifact.format
```

Execution parameters must be read through `context.params`:

```python
required_value = context.params.required("requiredParameter")
optional_value = context.params.get("optionalParameter", default_value)
```

Output artifacts must be written through logical output slots declared in `manifest.outputs`:

```python
context.outputs.write("outputSlot", output_bytes)
```

If a library needs to write directly to a file path, the handler may resolve the runtime-managed local output path:

```python
output_path = context.outputs.path("outputSlot")
```

The Worker Runtime resolves logical input and output slots declared in the Worker Manifest to runtime-managed local filesystem paths. It reads inputs from and writes outputs to those local paths only.

After the Worker process terminates successfully, the Argo `wait` container uploads the output artifacts declared in the generated Workflow specification to attempt-specific locations in RunArtifactStorage. The Worker Runtime does not access object storage directly or publish an authoritative terminal node state.

Conceptually, a worker handler follows this structure:

```python
class ExampleWorkerHandler:
    def execute(self, context: WorkerExecutionContext) -> None:
        input_a = context.inputs.read("inputSlotA")
        input_b_path = context.inputs.path("inputSlotB")
        required_parameter = context.params.required("requiredParameter")

        # Execute worker-specific business logic.
        output_bytes = run_worker_specific_logic(
            input_a,
            input_b_path,
            required_parameter,
        )

        context.outputs.write("outputSlot", output_bytes)
```

### Data flow

In Argo Workflows 4.0, each computational task normally creates a Pod containing three containers:

* the `init` container downloads the input artifacts to their configured local paths;
* the `main` container runs the user-provided Worker Image;
* the `wait` container uploads the declared output artifacts after the main container completes.


| DAG node or stage                                | Artifact source    | Artifact destination |
|--------------------------------------------------|--------------------|----------------------|
| System-generated `stage-inputs` Argo DAG task    | DataSource         | RunArtifactStorage   |
| Initial DAG node(s)                              | RunArtifactStorage | RunArtifactStorage   |
| Intermediate DAG node                            | RunArtifactStorage | RunArtifactStorage   |
| Final computational DAG node(s)                  | RunArtifactStorage | RunArtifactStorage   |
| System-generated `publish-results` Argo DAG task | RunArtifactStorage | ResultStorage        |

**RunArtifactStorage** is a run-scoped S3 artifact repository used by Argo to store staged DAG inputs and computational node outputs. Artifacts from different DAG Runs must use isolated storage keys.

#### Initial DAG node data flow

```mermaid
flowchart TD
    DATA_SOURCE(["DataSource"])
    RUN_STORAGE(["RunArtifactStorage"])

    subgraph STAGE_INPUTS["System-generated `stage-inputs` Argo DAG task"]
        STAGER_INPUTS["/opt/mdds/inputs/"]
        STAGER["Trusted Input Stager Process"]
        STAGER_OUTPUTS["/opt/mdds/outputs/"]

        STAGER_INPUTS --> STAGER
        STAGER -->|"Prepares inputs for staging"| STAGER_OUTPUTS
    end

    subgraph INITIAL_NODE["Initial computational DAG node"]
        INPUTS["/opt/mdds/inputs/"]
        WORKER["Worker Process"]
        OUTPUTS["/opt/mdds/outputs/"]

        INPUTS --> WORKER
        WORKER -->|"Writes local files"| OUTPUTS
    end

    DATA_SOURCE -->|"Argo downloads declared DAG inputs"| STAGER_INPUTS
    STAGER_OUTPUTS -->|"Argo uploads staged DAG inputs"| RUN_STORAGE
    RUN_STORAGE -->|"Argo downloads staged DAG inputs"| INPUTS
    OUTPUTS -->|"Argo uploads declared node outputs"| RUN_STORAGE
```

#### Intermediate DAG node data flow

```mermaid
flowchart TD
    RUN_STORAGE(["RunArtifactStorage"])
  subgraph INTERMEDIATE_NODE["Intermediate computational DAG node"]
      INPUTS["/opt/mdds/inputs/"]
      WORKER["Worker Process"]
      OUTPUTS["/opt/mdds/outputs/"]
  end
    

    RUN_STORAGE -->|"Argo downloads input artifacts"| INPUTS
    INPUTS --> WORKER
    WORKER -->|"Writes local files"| OUTPUTS
    OUTPUTS -->|"Argo uploads outputs"| RUN_STORAGE
```

#### Final DAG node and result publication data flow

```mermaid
flowchart TD
    RUN_STORAGE(["RunArtifactStorage"])
    RESULT_STORAGE(["ResultStorage"])

    subgraph FINAL_NODE["Final computational DAG node"]
        INPUTS["/opt/mdds/inputs/"]
        WORKER["Worker Process"]
        OUTPUTS["/opt/mdds/outputs/"]

        INPUTS --> WORKER
        WORKER -->|"Writes local files"| OUTPUTS
    end

    subgraph PUBLISH_NODE["System-generated `publish-results` Argo DAG task"]
        PUBLISH_INPUTS["/opt/mdds/inputs/"]
        PUBLISHER["Trusted Result Publisher Process"]
        PUBLISH_OUTPUTS["/opt/mdds/outputs/"]

        PUBLISH_INPUTS --> PUBLISHER
        PUBLISHER -->|"Prepares results for publication"| PUBLISH_OUTPUTS
    end

    RUN_STORAGE -->|"Argo downloads input artifacts"| INPUTS
    OUTPUTS -->|"Argo uploads declared DAG outputs"| RUN_STORAGE
    RUN_STORAGE -->|"Argo downloads declared DAG outputs"| PUBLISH_INPUTS
    PUBLISH_OUTPUTS -->|"Argo uploads published results"| RESULT_STORAGE
```
The diagram shows one output-producing computational node. A DAG Run may contain multiple producers of declared DAG outputs; publish-results receives artifacts from all such producers.

## Argo Workflow Observer

The Argo Workflow Observer periodically reads the authoritative Workflow state through the Argo Server REST API and reconciles it with the MDDS DAG Run state.
The initial observer strategy includes:

* periodic REST polling;
* reconciliation on server startup;
* reconciliation after uncertain submission.

## End-to-End Artifact Flow


```mermaid
flowchart TD
    USER_UPLOAD(["User"])
    DATA_SOURCE["DataSource<br/>Source of declared DAG inputs"]
    WORKER_POD["Worker Pod<br/>/opt/mdds/inputs<br/>/opt/mdds/outputs"]
    RUN_STORAGE["RunArtifactStorage<br/>Internal and intermediate artifacts"]
    DATA_STORAGE["ResultStorage<br/>Published final outputs"]
    USER_DOWNLOAD(["User"])

    USER_UPLOAD -->|"Uploads input data"| DATA_SOURCE
    DATA_SOURCE -->|"Trusted input preparation"| RUN_STORAGE
    RUN_STORAGE -->|"Argo stages input artifacts"| WORKER_POD
    WORKER_POD -->|"Argo stores node outputs"| RUN_STORAGE
    RUN_STORAGE -->|"Trusted result publication"| DATA_STORAGE
    DATA_STORAGE -->|"Downloads results"| USER_DOWNLOAD
```

## Resource Limits and Timeouts

Define policy for resource limits and timeouts.

```yaml
executionPolicy:
  timeout: PT30M

  resources:
    requests:
      cpu: "500m"
      memory: "512Mi"
      ephemeralStorage: "1Gi"

    limits:
      cpu: "2"
      memory: "4Gi"
      ephemeralStorage: "10Gi"
```


## Component Responsibilities

```mermaid
flowchart TD
    USER["User"]
    WEB_CLIENT["MDDS Web Client — user's browser"]

    subgraph K8S["Kubernetes / K3s"]
        WEB_SERVER["MDDS Server"]
        K8S_API["Kubernetes API: Workflow Custom Resource"]

        subgraph ARGO["Argo Workflows"]
            ARGO_SERVER["Argo Server REST API"]
            ARGO_CONTROLLER["Workflow Controller"]
        end

        K8S_POD["Argo-managed task Pods"]
        MINIO_S3[("MinIO / S3<br/>DataSource, RunArtifactStorage, ResultStorage")]
    end

    USER -->|"creates a DAG, selects data and worker implementations, and starts a run"| WEB_CLIENT
    WEB_CLIENT -->|"sends DAG definitions, user selections, and run commands"| WEB_SERVER
    WEB_SERVER -->|"compiles the DAG run into an Argo Workflow and submits it"| ARGO_SERVER
    ARGO_SERVER -->|"creates Workflow Custom Resource"| K8S_API
    ARGO_CONTROLLER -->|"watches Workflow Custom Resource and creates Pod resources"| K8S_API
    K8S_API -->|"Kubernetes schedules and manages task Pods"| K8S_POD
    K8S_POD <-->|"Argo artifact containers transfer artifacts"| MINIO_S3
```

The responsibilities are distributed as follows:

* **MDDS Web Client** — allows users to create a DAG and select data and worker implementations.
* **MDDS Server** — stores the MDDS model, creates a snapshot of a specific DAG run, compiles it into a single Argo Workflow, and submits it through the Argo Server REST API.
* **Argo Server** — accepts the Workflow and provides an API and UI.
* **Workflow Controller** — interprets the generated Argo Workflow and creates Pods for system and computational DAG tasks.
* **Kubernetes/K3s** — schedules and runs Argo-managed task Pods.
* **MinIO/S3** — stores input data, intermediate artifacts, and output artifacts.



## Architecture Decision Records

* **ADR-1**: MDDS uses the Argo Server REST API as its only execution-platform API. Communication uses HTTPS and JSON. gRPC is not used in MDDS v1.
* **ADR-2**: User-provided Worker Images never receive direct write access to either DataSource or ResultStorage. Workers operate only on local input and output paths. Argo and trusted MDDS platform components transfer artifacts between local paths and object storage.
* **ADR-3**: Intermediate artifacts are written only to run-specific, attempt-specific namespaces. After all computational DAG nodes complete successfully, the system-generated `publish-results` Argo DAG task publishes all declared DAG outputs from RunArtifactStorage to their configured ResultStorage destinations. The Workflow succeeds only after result publication succeeds.
* **ADR-4**: Storage credentials are mounted only into trusted artifact-transfer or publication components. They are never included in the Worker Manifest or mounted into the user-provided Worker container.
* **ADR-5**: An OCI Worker Image does not contain input data and does not receive input data during the image build process.
* **ADR-6**: Argo stages input artifacts into the running Worker Pod before the Worker Process starts.
* **ADR-7**: The Worker operates only on local filesystem paths under `/opt/mdds` and does not interact directly with object storage.
* **ADR-8**: Each generated Argo template explicitly declares all input and output artifacts together with their corresponding local filesystem paths.
* **ADR-9**: Argo Workflows is the sole owner of automatic Node Attempt retries. The generated Workflow defines retry eligibility and exposes Argo’s retry index to both artifact-location templates and the Worker Runtime. The canonical MDDS attempt identifier is derived from that index.
* **ADR-10**: Every standalone MDDS configuration and runtime document declares its contract through the top-level `apiVersion` and `kind` fields. `apiVersion` identifies the versioned schema and semantics, while `kind` identifies the document type. Embedded objects inherit the contract of the enclosing document and do not repeat these fields.
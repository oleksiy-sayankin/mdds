# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import json
from dataclasses import dataclass
from pathlib import Path

from mdds_worker_runtime_common.config import WorkerConfig
from mdds_worker_runtime_common.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime_common.domain.manifest import WorkerManifest
from mdds_worker_runtime_common.execution.artifacts import (
    Execution,
    InputArtifacts,
    OutputArtifacts,
    PreparedInputArtifact,
    PreparedOutputArtifact,
    WorkerParameters,
)

WORKER_EXECUTION_CONTEXT_SNAPSHOT_PATH = Path("/opt/mdds/tmp/context-snapshot.json")


@dataclass(frozen=True)
class WorkerExecutionContext:
    execution: Execution
    inputs: InputArtifacts
    params: WorkerParameters
    outputs: OutputArtifacts


def create_worker_execution_context(
    manifest: WorkerManifest,
    config: WorkerConfig,
) -> WorkerExecutionContext:
    """Create a handler-facing context from a manifest and configuration."""
    if manifest is None:
        raise ValueError("manifest cannot be null.")
    if config is None:
        raise ValueError("config cannot be null.")

    return WorkerExecutionContext(
        execution=Execution(
            user_id=manifest.execution.user_id,
            dag_run_id=manifest.execution.dag_run_id,
            node_id=manifest.execution.node_id,
            attempt_id=f"attempt-{config.argo_retry_index}",
        ),
        inputs=InputArtifacts(
            {
                slot: PreparedInputArtifact(
                    local_path=artifact.path,
                    format=artifact.format,
                )
                for slot, artifact in manifest.inputs.items()
            }
        ),
        params=WorkerParameters(manifest.params),
        outputs=OutputArtifacts(
            {
                slot: PreparedOutputArtifact(
                    local_path=artifact.path,
                    format=artifact.format,
                )
                for slot, artifact in manifest.outputs.items()
            }
        ),
    )


def persist(context: WorkerExecutionContext) -> None:
    """Persist the context for the supervised Worker process."""
    snapshot = {
        "execution": {
            "userId": context.execution.user_id,
            "dagRunId": context.execution.dag_run_id,
            "nodeId": context.execution.node_id,
            "attemptId": context.execution.attempt_id,
        },
        "inputs": {
            slot: {
                "localPath": str(artifact.local_path),
                "format": artifact.format.value,
            }
            for slot, artifact in context.inputs.items()
        },
        "params": dict(context.params.items()),
        "outputs": {
            slot: {
                "localPath": str(artifact.local_path),
                "format": artifact.format.value,
            }
            for slot, artifact in context.outputs.items()
        },
    }

    snapshot_path = WORKER_EXECUTION_CONTEXT_SNAPSHOT_PATH
    temporary_path = snapshot_path.with_suffix(f"{snapshot_path.suffix}.tmp")
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, allow_nan=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary_path.replace(snapshot_path)


def load_worker_execution_context() -> WorkerExecutionContext:
    """Load the context persisted for the supervised Worker process."""
    snapshot = json.loads(
        WORKER_EXECUTION_CONTEXT_SNAPSHOT_PATH.read_text(encoding="utf-8")
    )
    execution = snapshot["execution"]

    return WorkerExecutionContext(
        execution=Execution(
            user_id=execution["userId"],
            dag_run_id=execution["dagRunId"],
            node_id=execution["nodeId"],
            attempt_id=execution["attemptId"],
        ),
        inputs=InputArtifacts(
            {
                slot: PreparedInputArtifact(
                    local_path=Path(artifact["localPath"]),
                    format=ArtifactFormat.from_raw(artifact["format"]),
                )
                for slot, artifact in snapshot["inputs"].items()
            }
        ),
        params=WorkerParameters(snapshot["params"]),
        outputs=OutputArtifacts(
            {
                slot: PreparedOutputArtifact(
                    local_path=Path(artifact["localPath"]),
                    format=ArtifactFormat.from_raw(artifact["format"]),
                )
                for slot, artifact in snapshot["outputs"].items()
            }
        ),
    )

# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import json
from pathlib import Path

import pytest
from mdds_worker_runtime_common.config import WorkerConfig
from mdds_worker_runtime_common.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime_common.domain.manifest import (
    ArtifactRef,
    NodeRunIdentity,
    WorkerManifest,
)
from mdds_worker_runtime_common.execution import context as context_module
from mdds_worker_runtime_common.execution.artifacts import (
    Execution,
    PreparedInputArtifact,
    PreparedOutputArtifact,
)
from mdds_worker_runtime_common.execution.context import (
    WorkerExecutionContext,
    create_worker_execution_context,
    load_worker_execution_context,
    persist,
)


@pytest.fixture
def manifest(tmp_path: Path) -> WorkerManifest:
    return WorkerManifest(
        api_version="mdds/v1",
        kind="WorkerManifest",
        execution=NodeRunIdentity(
            user_id=12345,
            dag_run_id="dag-run-1",
            node_id="solve-a",
        ),
        inputs={
            "matrix": ArtifactRef(
                path=tmp_path / "inputs" / "matrix.csv",
                format=ArtifactFormat.CSV,
            ),
            "rhs": ArtifactRef(
                path=tmp_path / "inputs" / "rhs.csv",
                format=ArtifactFormat.CSV,
            ),
        },
        params={
            "solvingMethod": "numpy_exact_solver",
            "tolerance": 1e-8,
        },
        outputs={
            "solution": ArtifactRef(
                path=tmp_path / "outputs" / "solution.csv",
                format=ArtifactFormat.CSV,
            ),
        },
    )


@pytest.fixture
def config() -> WorkerConfig:
    return WorkerConfig(
        worker_name="python-worker-solving-slae",
        worker_version="0.1.0",
        worker_handler="mdds_slae_worker.handler:SlaeWorkerHandler",
        argo_retry_index=2,
    )


@pytest.fixture
def context_snapshot_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    snapshot_path = tmp_path / "snapshot" / "context-snapshot.json"
    monkeypatch.setattr(
        context_module,
        "WORKER_EXECUTION_CONTEXT_SNAPSHOT_PATH",
        snapshot_path,
    )
    return snapshot_path


@pytest.fixture
def worker_execution_context(
    manifest: WorkerManifest,
    config: WorkerConfig,
) -> WorkerExecutionContext:
    return create_worker_execution_context(manifest, config)


def test_creates_context_from_manifest_and_config(
    manifest: WorkerManifest,
    config: WorkerConfig,
) -> None:
    context = create_worker_execution_context(manifest, config)

    assert context.execution.user_id == 12345
    assert context.execution.dag_run_id == "dag-run-1"
    assert context.execution.node_id == "solve-a"
    assert context.execution.attempt_id == "attempt-2"

    assert context.inputs.get("matrix") == PreparedInputArtifact(
        local_path=manifest.inputs["matrix"].path,
        format=ArtifactFormat.CSV,
    )
    assert context.inputs.path("rhs") == manifest.inputs["rhs"].path

    assert context.params.required("solvingMethod") == "numpy_exact_solver"
    assert context.params.get("tolerance") == 1e-8

    assert context.outputs.get("solution") == PreparedOutputArtifact(
        local_path=manifest.outputs["solution"].path,
        format=ArtifactFormat.CSV,
    )


def test_create_does_not_touch_artifact_files(
    manifest: WorkerManifest,
    config: WorkerConfig,
) -> None:
    context = create_worker_execution_context(manifest, config)

    assert not context.inputs.path("matrix").exists()
    assert not context.outputs.path("solution").parent.exists()


def test_rejects_null_manifest(config: WorkerConfig) -> None:
    with pytest.raises(ValueError, match="manifest cannot be null"):
        create_worker_execution_context(None, config)  # type: ignore[arg-type]


def test_rejects_null_config(manifest: WorkerManifest) -> None:
    with pytest.raises(ValueError, match="config cannot be null"):
        create_worker_execution_context(manifest, None)  # type: ignore[arg-type]


def test_persist_writes_context_as_json_snapshot(
    worker_execution_context: WorkerExecutionContext,
    context_snapshot_path: Path,
) -> None:
    persist(worker_execution_context)

    assert json.loads(context_snapshot_path.read_text(encoding="utf-8")) == {
        "execution": {
            "userId": 12345,
            "dagRunId": "dag-run-1",
            "nodeId": "solve-a",
            "attemptId": "attempt-2",
        },
        "inputs": {
            "matrix": {
                "localPath": str(worker_execution_context.inputs.path("matrix")),
                "format": "csv",
            },
            "rhs": {
                "localPath": str(worker_execution_context.inputs.path("rhs")),
                "format": "csv",
            },
        },
        "params": {
            "solvingMethod": "numpy_exact_solver",
            "tolerance": 1e-8,
        },
        "outputs": {
            "solution": {
                "localPath": str(worker_execution_context.outputs.path("solution")),
                "format": "csv",
            },
        },
    }
    assert not context_snapshot_path.with_suffix(".json.tmp").exists()


def test_load_reads_context_from_json_snapshot(
    context_snapshot_path: Path,
) -> None:
    input_path = context_snapshot_path.parent / "inputs" / "payload.json"
    output_path = context_snapshot_path.parent / "outputs" / "result.csv"
    context_snapshot_path.parent.mkdir(parents=True)
    context_snapshot_path.write_text(
        json.dumps(
            {
                "execution": {
                    "userId": 98765,
                    "dagRunId": "dag-run-load",
                    "nodeId": "transform-a",
                    "attemptId": "attempt-4",
                },
                "inputs": {
                    "payload": {
                        "localPath": str(input_path),
                        "format": "json",
                    },
                },
                "params": {
                    "title": "Ð¢ÐµÑÑ‚Ð¾Ð²Ñ‹Ð¹ Ð·Ð°Ð¿ÑƒÑÐº",
                    "options": {
                        "enabled": True,
                        "threshold": None,
                    },
                    "values": [1, 2, 3],
                },
                "outputs": {
                    "result": {
                        "localPath": str(output_path),
                        "format": "csv",
                    },
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    context = load_worker_execution_context()

    assert context.execution == Execution(
        user_id=98765,
        dag_run_id="dag-run-load",
        node_id="transform-a",
        attempt_id="attempt-4",
    )
    assert context.inputs.get("payload") == PreparedInputArtifact(
        local_path=input_path,
        format=ArtifactFormat.JSON,
    )
    assert context.params.required("title") == "Ð¢ÐµÑÑ‚Ð¾Ð²Ñ‹Ð¹ Ð·Ð°Ð¿ÑƒÑÐº"
    assert context.params.required("options") == {
        "enabled": True,
        "threshold": None,
    }
    assert context.params.required("values") == [1, 2, 3]
    assert context.outputs.get("result") == PreparedOutputArtifact(
        local_path=output_path,
        format=ArtifactFormat.CSV,
    )


def test_persist_and_load_round_trip(
    worker_execution_context: WorkerExecutionContext,
    context_snapshot_path: Path,
) -> None:
    persist(worker_execution_context)

    assert load_worker_execution_context() == worker_execution_context

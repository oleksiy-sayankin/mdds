# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import json

import pytest

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.execution.artifacts import (
    InputArtifacts,
    JobParameters,
    OutputArtifacts,
    PreparedInputArtifact,
    PreparedOutputArtifact,
)
from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.context_snapshot import (
    ArtifactSnapshot,
    JobExecutionContextSnapshot,
    JobExecutionContextSnapshotError,
    JobExecutionContextSnapshotStore,
)


def test_context_snapshot_store_saves_and_loads_context(tmp_path) -> None:
    context = _create_context(tmp_path)
    snapshot_path = tmp_path / "snapshots" / "context.snapshot.json"

    JobExecutionContextSnapshotStore().save(context, snapshot_path)

    loaded_context = JobExecutionContextSnapshotStore().load(snapshot_path)

    assert loaded_context.user_id == 42
    assert loaded_context.job_id == "job-1"
    assert loaded_context.job_type == "SOLVING_SLAE"
    assert loaded_context.work_dir == tmp_path / "jobs" / "42" / "job-1"
    assert loaded_context.input_dir == tmp_path / "jobs" / "42" / "job-1" / "in"
    assert loaded_context.output_dir == tmp_path / "jobs" / "42" / "job-1" / "out"

    assert (
        loaded_context.inputs.get("matrix").object_key == "jobs/42/job-1/in/matrix.csv"
    )
    assert (
        loaded_context.inputs.path("matrix")
        == tmp_path / "jobs" / "42" / "job-1" / "in" / "matrix.csv"
    )
    assert loaded_context.inputs.get("matrix").format == ArtifactFormat.CSV

    assert (
        loaded_context.outputs.get("solution").object_key
        == "jobs/42/job-1/out/solution.csv"
    )
    assert (
        loaded_context.outputs.path("solution")
        == tmp_path / "jobs" / "42" / "job-1" / "out" / "solution.csv"
    )
    assert loaded_context.outputs.get("solution").format == ArtifactFormat.CSV

    assert loaded_context.params.required("solvingMethod") == "numpy_exact_solver"


def test_context_snapshot_store_rejects_null_context(tmp_path) -> None:
    with pytest.raises(ValueError, match="context cannot be null."):
        JobExecutionContextSnapshotStore().save(
            None,  # type: ignore[arg-type]
            tmp_path / "context.snapshot.json",
        )


def test_context_snapshot_store_rejects_null_path(tmp_path) -> None:
    context = _create_context(tmp_path)
    snapshot_store = JobExecutionContextSnapshotStore()

    with pytest.raises(ValueError, match="path cannot be null."):
        snapshot_store.save(
            context,
            None,  # type: ignore[arg-type]
        )

    with pytest.raises(ValueError, match="path cannot be null."):
        snapshot_store.load(None)  # type: ignore[arg-type]


def test_context_snapshot_store_rejects_unsupported_version(tmp_path) -> None:
    context = _create_context(tmp_path)
    snapshot_path = tmp_path / "context.snapshot.json"

    snapshot_store = JobExecutionContextSnapshotStore()
    snapshot_store.save(context, snapshot_path)

    snapshot_data = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot_data["snapshotVersion"] = 999
    snapshot_path.write_text(json.dumps(snapshot_data), encoding="utf-8")

    with pytest.raises(
        JobExecutionContextSnapshotError,
        match="Unsupported JobExecutionContext snapshot version: 999",
    ):
        snapshot_store.load(snapshot_path)


def _create_context(tmp_path) -> JobExecutionContext:
    work_dir = tmp_path / "jobs" / "42" / "job-1"
    input_dir = work_dir / "in"
    output_dir = work_dir / "out"

    return JobExecutionContext(
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        work_dir=work_dir,
        input_dir=input_dir,
        output_dir=output_dir,
        inputs=InputArtifacts(
            {
                "matrix": PreparedInputArtifact(
                    object_key="jobs/42/job-1/in/matrix.csv",
                    local_path=input_dir / "matrix.csv",
                    format=ArtifactFormat.CSV,
                ),
                "rhs": PreparedInputArtifact(
                    object_key="jobs/42/job-1/in/rhs.csv",
                    local_path=input_dir / "rhs.csv",
                    format=ArtifactFormat.CSV,
                ),
            }
        ),
        outputs=OutputArtifacts(
            {
                "solution": PreparedOutputArtifact(
                    object_key="jobs/42/job-1/out/solution.csv",
                    local_path=output_dir / "solution.csv",
                    format=ArtifactFormat.CSV,
                ),
            }
        ),
        params=JobParameters(
            {
                "solvingMethod": "numpy_exact_solver",
            }
        ),
    )


def test_context_snapshot_rejects_null_context() -> None:
    with pytest.raises(ValueError, match="context cannot be null."):
        JobExecutionContextSnapshot.from_context(None)  # type: ignore[arg-type]


def test_context_snapshot_store_wraps_json_serialization_error(
    tmp_path,
    monkeypatch,
) -> None:
    context = _create_context(tmp_path)
    snapshot_path = tmp_path / "context.snapshot.json"

    def raise_type_error(*_args, **_kwargs):
        raise TypeError("not serializable")

    monkeypatch.setattr(
        "mdds_worker_runtime.execution.context_snapshot.json.dumps",
        raise_type_error,
    )

    with pytest.raises(
        JobExecutionContextSnapshotError,
        match="JobExecutionContext snapshot is not JSON serializable",
    ):
        JobExecutionContextSnapshotStore().save(context, snapshot_path)


def test_context_snapshot_store_wraps_write_error(
    tmp_path,
    monkeypatch,
) -> None:
    context = _create_context(tmp_path)
    snapshot_path = tmp_path / "context.snapshot.json"

    def raise_os_error(self, *_args, **_kwargs):
        raise OSError("disk error")

    monkeypatch.setattr(
        type(snapshot_path),
        "write_text",
        raise_os_error,
    )

    with pytest.raises(
        JobExecutionContextSnapshotError,
        match="Cannot save JobExecutionContext snapshot",
    ):
        JobExecutionContextSnapshotStore().save(context, snapshot_path)


def test_context_snapshot_store_wraps_read_error(tmp_path) -> None:
    snapshot_path = tmp_path / "missing.snapshot.json"

    with pytest.raises(
        JobExecutionContextSnapshotError,
        match="Cannot read JobExecutionContext snapshot",
    ):
        JobExecutionContextSnapshotStore().load(snapshot_path)


def test_context_snapshot_store_wraps_json_parse_error(tmp_path) -> None:
    snapshot_path = tmp_path / "broken.snapshot.json"
    snapshot_path.write_text("{not-valid-json", encoding="utf-8")

    with pytest.raises(
        JobExecutionContextSnapshotError,
        match="Cannot parse JobExecutionContext snapshot",
    ):
        JobExecutionContextSnapshotStore().load(snapshot_path)


def test_context_snapshot_store_wraps_invalid_snapshot_error(tmp_path) -> None:
    snapshot_path = tmp_path / "invalid.snapshot.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "snapshotVersion": 1,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        JobExecutionContextSnapshotError,
        match="Invalid JobExecutionContext snapshot",
    ):
        JobExecutionContextSnapshotStore().load(snapshot_path)


def test_artifact_snapshot_rejects_null_artifact_format(tmp_path) -> None:
    artifact = PreparedInputArtifact(
        object_key="jobs/42/job-1/in/matrix.csv",
        local_path=tmp_path / "matrix.csv",
        format=None,  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="artifact format cannot be null."):
        ArtifactSnapshot.from_input_artifact(artifact)

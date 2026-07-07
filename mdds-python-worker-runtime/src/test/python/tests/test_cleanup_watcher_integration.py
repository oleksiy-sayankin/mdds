# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import time
from unittest.mock import MagicMock

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.domain.manifest import ArtifactRef, JobManifest
from mdds_worker_runtime.execution.artifacts import (
    InputArtifacts,
    JobParameters,
    OutputArtifacts,
    PreparedInputArtifact,
    PreparedOutputArtifact,
)
from mdds_worker_runtime.execution.cleanup_watcher import CleanupWatcher
from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.context_snapshot import (
    JobExecutionContextSnapshotStore,
)
from mdds_worker_runtime.execution.models import (
    ExecutionRecord,
    WorkerJobStatus,
    ProcessRecord,
)
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.execution.supervisor import CONTEXT_SNAPSHOT_FILE_NAME
from mdds_worker_runtime.execution.workspace import JobWorkspace
from mdds_worker_runtime.job_state import (
    JobStateTransitionCoordinator,
)

FIXED_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
WORKER_ID = "worker-1"
USER_ID = 42


def test_cleanup_watcher_removes_only_cleanup_ready_records_and_local_workspaces(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    coordinator = JobStateTransitionCoordinator()

    cleanup_ready_done = _create_registered_job(
        tmp_path=tmp_path,
        registry=registry,
        coordinator=coordinator,
        job_id="cleanup-ready-done",
        job_status=WorkerJobStatus.DONE,
    )
    cleanup_ready_error = _create_registered_job(
        tmp_path=tmp_path,
        registry=registry,
        coordinator=coordinator,
        job_id="cleanup-ready-error",
        job_status=WorkerJobStatus.ERROR,
    )
    running = _create_registered_job(
        tmp_path=tmp_path,
        registry=registry,
        coordinator=coordinator,
        job_id="running-job",
        job_status=WorkerJobStatus.IN_PROGRESS,
    )

    watcher = CleanupWatcher(
        execution_registry=registry,
        job_state_transition_coordinator=coordinator,
        worker_id=WORKER_ID,
        cleanup_interval_seconds=0.01,
    )

    watcher.start()

    try:
        _wait_until(
            lambda: _assert_cleanup_finished(
                registry=registry,
                coordinator=coordinator,
                removed_jobs=[cleanup_ready_done, cleanup_ready_error],
                preserved_jobs=[running],
            ),
            timeout_seconds=2.0,
            interval_seconds=0.01,
        )
    finally:
        watcher.stop(timeout_seconds=1.0)

    _assert_job_workspace_removed(cleanup_ready_done)
    _assert_job_workspace_removed(cleanup_ready_error)

    _assert_job_workspace_preserved(running)

    assert registry.get(cleanup_ready_done.job_id) is None
    assert registry.get(cleanup_ready_error.job_id) is None
    assert registry.get(running.job_id) is running.record

    assert coordinator.get_state(cleanup_ready_done.job_id) is None
    assert coordinator.get_state(cleanup_ready_error.job_id) is None
    assert coordinator.get_state(running.job_id) is WorkerJobStatus.IN_PROGRESS

    assert registry.size() == 1


def _create_registered_job(
    *,
    tmp_path: Path,
    registry: ExecutionRegistry,
    coordinator: JobStateTransitionCoordinator,
    job_id: str,
    job_status: "WorkerJobStatus",
) -> "_JobFixture":
    context = _context(tmp_path, job_id=job_id)
    _create_production_like_workspace(context)

    record = ExecutionRecord(
        workspace=context.workspace,
        context=context,
        process_record=ProcessRecord(
            process=MagicMock(),
            parent_connection=MagicMock(),
            started_at=FIXED_TIME,
        ),
    )

    registry.add(record)
    _apply_lifecycle_state(
        coordinator=coordinator,
        execution_record=record,
        job_status=job_status,
    )

    return _JobFixture(
        job_id=job_id,
        record=record,
        work_dir=context.work_dir,
        input_dir=context.input_dir,
        output_dir=context.output_dir,
        manifest_path=context.work_dir / "manifest.json",
        context_snapshot_path=context.work_dir / CONTEXT_SNAPSHOT_FILE_NAME,
        matrix_path=context.input_path("matrix"),
        rhs_path=context.input_path("rhs"),
        solution_path=context.output_path("solution"),
    )


def _apply_lifecycle_state(
    *,
    coordinator: JobStateTransitionCoordinator,
    execution_record: ExecutionRecord,
    job_status: "WorkerJobStatus",
) -> None:
    record = coordinator.create(
        execution_record.job_id,
        MagicMock(),
    )
    with record.lock:
        record.state = job_status


def _create_production_like_workspace(context: JobExecutionContext) -> None:
    context.input_dir.mkdir(parents=True)
    context.output_dir.mkdir(parents=True)

    context.input_path("matrix").write_text(
        "1,0\n0,1\n",
        encoding="utf-8",
    )
    context.input_path("rhs").write_text(
        "7\n11\n",
        encoding="utf-8",
    )
    context.output_path("solution").write_text(
        "7\n11\n",
        encoding="utf-8",
    )

    (context.work_dir / "manifest.json").write_text(
        _manifest_json(context),
        encoding="utf-8",
    )

    JobExecutionContextSnapshotStore().save(
        context,
        context.work_dir / CONTEXT_SNAPSHOT_FILE_NAME,
    )


def _manifest_json(context: JobExecutionContext) -> str:
    return (
        "{\n"
        '  "manifestVersion": 1,\n'
        f'  "userId": {context.user_id},\n'
        f'  "jobId": "{context.job_id}",\n'
        f'  "jobType": "{context.job_type}",\n'
        '  "inputs": {\n'
        '    "matrix": {\n'
        f'      "objectKey": "jobs/{context.user_id}/{context.job_id}/in/matrix.csv",\n'
        '      "format": "csv"\n'
        "    },\n"
        '    "rhs": {\n'
        f'      "objectKey": "jobs/{context.user_id}/{context.job_id}/in/rhs.csv",\n'
        '      "format": "csv"\n'
        "    }\n"
        "  },\n"
        '  "params": {\n'
        '    "solvingMethod": "numpy_exact_solver"\n'
        "  },\n"
        '  "outputs": {\n'
        '    "solution": {\n'
        f'      "objectKey": "jobs/{context.user_id}/{context.job_id}/out/solution.csv",\n'
        '      "format": "csv"\n'
        "    }\n"
        "  }\n"
        "}\n"
    )


def _assert_cleanup_finished(
    *,
    registry: ExecutionRegistry,
    coordinator: JobStateTransitionCoordinator,
    removed_jobs: list["_JobFixture"],
    preserved_jobs: list["_JobFixture"],
) -> None:
    for fixture in removed_jobs:
        assert registry.get(fixture.job_id) is None
        assert coordinator.get_state(fixture.job_id) is None
        assert not fixture.work_dir.exists()

    for fixture in preserved_jobs:
        assert registry.get(fixture.job_id) is fixture.record
        assert coordinator.get_state(fixture.job_id) is not None
        assert fixture.work_dir.exists()

    assert registry.size() == len(preserved_jobs)


def _assert_job_workspace_removed(fixture: "_JobFixture") -> None:
    assert not fixture.work_dir.exists()
    assert not fixture.input_dir.exists()
    assert not fixture.output_dir.exists()
    assert not fixture.manifest_path.exists()
    assert not fixture.context_snapshot_path.exists()
    assert not fixture.matrix_path.exists()
    assert not fixture.rhs_path.exists()
    assert not fixture.solution_path.exists()


def _assert_job_workspace_preserved(fixture: "_JobFixture") -> None:
    assert fixture.work_dir.exists()
    assert fixture.input_dir.exists()
    assert fixture.output_dir.exists()

    assert fixture.manifest_path.exists()
    assert fixture.context_snapshot_path.exists()
    assert fixture.matrix_path.exists()
    assert fixture.rhs_path.exists()
    assert fixture.solution_path.exists()

    assert fixture.matrix_path.read_text(encoding="utf-8") == "1,0\n0,1\n"
    assert fixture.rhs_path.read_text(encoding="utf-8") == "7\n11\n"
    assert fixture.solution_path.read_text(encoding="utf-8") == "7\n11\n"


def _wait_until(
    assertion,
    *,
    timeout_seconds: float,
    interval_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: AssertionError | None = None

    while time.monotonic() < deadline:
        try:
            assertion()
            return
        except AssertionError as exc:
            last_error = exc
            time.sleep(interval_seconds)

    if last_error is not None:
        raise last_error

    raise AssertionError("Condition was not satisfied before timeout.")


def _context(tmp_path: Path, *, job_id: str) -> JobExecutionContext:
    work_dir = tmp_path / "jobs" / str(USER_ID) / job_id
    input_dir = work_dir / "in"
    output_dir = work_dir / "out"

    manifest = JobManifest(
        manifest_version=1,
        user_id=USER_ID,
        job_id=job_id,
        job_type="SOLVING_SLAE",
        inputs={
            "matrix": ArtifactRef(
                object_key=f"jobs/{USER_ID}/{job_id}/in/matrix.csv",
                format=ArtifactFormat.CSV,
            ),
            "rhs": ArtifactRef(
                object_key=f"jobs/{USER_ID}/{job_id}/in/rhs.csv",
                format=ArtifactFormat.CSV,
            ),
        },
        params={
            "solvingMethod": "numpy_exact_solver",
        },
        outputs={
            "solution": ArtifactRef(
                object_key=f"jobs/{USER_ID}/{job_id}/out/solution.csv",
                format=ArtifactFormat.CSV,
            ),
        },
    )

    workspace = JobWorkspace(
        manifest=manifest,
        work_dir=work_dir,
        input_dir=input_dir,
        output_dir=output_dir,
        worker_id=WORKER_ID,
    )

    return JobExecutionContext(
        workspace=workspace,
        inputs=InputArtifacts(
            {
                "matrix": PreparedInputArtifact(
                    object_key=f"jobs/{USER_ID}/{job_id}/in/matrix.csv",
                    local_path=input_dir / "matrix.csv",
                    format=ArtifactFormat.CSV,
                ),
                "rhs": PreparedInputArtifact(
                    object_key=f"jobs/{USER_ID}/{job_id}/in/rhs.csv",
                    local_path=input_dir / "rhs.csv",
                    format=ArtifactFormat.CSV,
                ),
            }
        ),
        outputs=OutputArtifacts(
            {
                "solution": PreparedOutputArtifact(
                    object_key=f"jobs/{USER_ID}/{job_id}/out/solution.csv",
                    local_path=output_dir / "solution.csv",
                    format=ArtifactFormat.CSV,
                ),
            }
        ),
        params=JobParameters(manifest.params),
    )


@dataclass(frozen=True)
class _JobFixture:
    job_id: str
    record: ExecutionRecord
    work_dir: Path
    input_dir: Path
    output_dir: Path
    manifest_path: Path
    context_snapshot_path: Path
    matrix_path: Path
    rhs_path: Path
    solution_path: Path

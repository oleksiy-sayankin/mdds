# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.domain.manifest import JobManifest, ArtifactRef
from mdds_worker_runtime.execution.artifacts import (
    InputArtifacts,
    JobParameters,
    OutputArtifacts,
    PreparedInputArtifact,
    PreparedOutputArtifact,
)
from mdds_worker_runtime.execution.cleanup_watcher import CleanupWatcher
from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.models import ExecutionRecord, WorkerJobStatus
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.execution.workspace import JobWorkspace
from mdds_worker_runtime.job_state import (
    JobStateTransitionCoordinator,
)
from mdds_worker_runtime.queue.queue_client import Acknowledger

FIXED_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
WORKER_ID = "worker-1"
JOB_ID = "job-1"


@pytest.mark.parametrize(
    ("field_name", "bad_value", "error_message"),
    [
        ("execution_registry", None, "execution_registry cannot be null."),
        (
            "job_state_transition_coordinator",
            None,
            "job_state_transition_coordinator cannot be null.",
        ),
        ("worker_id", None, "worker_id cannot be null or blank."),
        ("worker_id", "", "worker_id cannot be null or blank."),
        ("worker_id", " ", "worker_id cannot be null or blank."),
        (
            "cleanup_interval_seconds",
            0,
            "cleanup_interval_seconds must be greater than zero.",
        ),
        (
            "cleanup_interval_seconds",
            -1,
            "cleanup_interval_seconds must be greater than zero.",
        ),
    ],
)
def test_cleanup_watcher_rejects_invalid_constructor_arguments(
    field_name: str,
    bad_value: object,
    error_message: str,
) -> None:
    kwargs = {
        "execution_registry": ExecutionRegistry(),
        "job_state_transition_coordinator": JobStateTransitionCoordinator(),
        "worker_id": WORKER_ID,
        "cleanup_interval_seconds": 0.01,
        field_name: bad_value,
    }

    with pytest.raises(ValueError) as exc_info:
        CleanupWatcher(**kwargs)

    assert str(exc_info.value) == error_message


def test_cleanup_watcher_ignores_record_without_worker_local_state(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    coordinator = JobStateTransitionCoordinator()
    record = _record(tmp_path)
    _create_work_dir(record)
    registry.add(record)

    watcher = _watcher(registry, coordinator)

    watcher.poll_once()

    assert record.context.work_dir.exists()
    assert registry.get(JOB_ID) is record
    assert coordinator.get_state(JOB_ID) is None
    assert registry.size() == 1


@pytest.mark.parametrize(
    "state",
    [
        WorkerJobStatus.SUBMITTED,
        WorkerJobStatus.INPUTS_PREPARED,
        WorkerJobStatus.VALIDATED,
        WorkerJobStatus.IN_PROGRESS,
    ],
)
def test_cleanup_watcher_ignores_non_terminal_worker_local_states(
    tmp_path: Path,
    state: WorkerJobStatus,
) -> None:
    registry = ExecutionRegistry()
    coordinator = _coordinator_in_state(state)
    record = _record(tmp_path)
    _create_work_dir(record)
    registry.add(record)

    watcher = _watcher(registry, coordinator)

    watcher.poll_once()

    assert record.context.work_dir.exists()
    assert registry.get(JOB_ID) is record
    assert coordinator.get_state(JOB_ID) is state
    assert registry.size() == 1


@pytest.mark.parametrize(
    "state",
    [
        WorkerJobStatus.DONE,
        WorkerJobStatus.ERROR,
        WorkerJobStatus.CANCELLED,
    ],
)
def test_cleanup_watcher_deletes_work_dir_and_removes_terminal_record_and_state(
    tmp_path: Path,
    state: WorkerJobStatus,
) -> None:
    registry = ExecutionRegistry()
    coordinator = _coordinator_in_state(state)
    record = _record(tmp_path)
    _create_work_dir(record)
    registry.add(record)

    watcher = _watcher(registry, coordinator)

    watcher.poll_once()

    assert not record.context.work_dir.exists()
    assert registry.get(JOB_ID) is None
    assert coordinator.get_state(JOB_ID) is None
    assert registry.size() == 0


def test_cleanup_watcher_removes_terminal_record_when_work_dir_is_already_absent(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    coordinator = _coordinator_in_state(WorkerJobStatus.DONE)
    record = _record(tmp_path)
    registry.add(record)

    watcher = _watcher(registry, coordinator)

    watcher.poll_once()

    assert not record.context.work_dir.exists()
    assert registry.get(JOB_ID) is None
    assert coordinator.get_state(JOB_ID) is None
    assert registry.size() == 0


def test_cleanup_watcher_keeps_terminal_record_and_state_when_work_dir_is_file(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    coordinator = _coordinator_in_state(WorkerJobStatus.DONE)
    work_dir = tmp_path / "jobs" / "42" / JOB_ID
    work_dir.parent.mkdir(parents=True)
    work_dir.write_text("not a directory", encoding="utf-8")

    record = _record(tmp_path, work_dir=work_dir)
    registry.add(record)

    watcher = _watcher(registry, coordinator)

    watcher.poll_once()

    assert work_dir.exists()
    assert work_dir.is_file()
    assert registry.get(JOB_ID) is record
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.DONE
    assert registry.size() == 1


def test_cleanup_watcher_keeps_terminal_record_and_state_when_rmtree_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = ExecutionRegistry()
    coordinator = _coordinator_in_state(WorkerJobStatus.ERROR)
    record = _record(tmp_path)
    _create_work_dir(record)
    registry.add(record)

    def fail_rmtree(_path: Path) -> None:
        raise RuntimeError("delete failed")

    monkeypatch.setattr(
        "mdds_worker_runtime.execution.cleanup_watcher.shutil.rmtree",
        fail_rmtree,
    )

    watcher = _watcher(registry, coordinator)

    watcher.poll_once()

    assert record.context.work_dir.exists()
    assert registry.get(JOB_ID) is record
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.ERROR
    assert registry.size() == 1


def test_cleanup_watcher_does_not_remove_state_when_record_remove_is_skipped(
    tmp_path: Path,
) -> None:
    coordinator = _coordinator_in_state(WorkerJobStatus.DONE)
    record = _record(tmp_path)
    _create_work_dir(record)

    registry = MagicMock()
    registry.snapshot.return_value = [record]
    registry.remove_if_same.return_value = False

    watcher = _watcher(registry, coordinator)

    watcher.poll_once()

    assert not record.context.work_dir.exists()
    registry.remove_if_same.assert_called_once_with(record)
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.DONE


def test_cleanup_watcher_ignores_none_record() -> None:
    registry = MagicMock()
    coordinator = JobStateTransitionCoordinator()
    registry.snapshot.return_value = [None]

    watcher = _watcher(registry, coordinator)

    watcher.poll_once()

    registry.remove_if_same.assert_not_called()


def test_cleanup_watcher_start_stop_are_idempotent() -> None:
    watcher = _watcher(
        ExecutionRegistry(),
        JobStateTransitionCoordinator(),
        cleanup_interval_seconds=0.01,
    )

    watcher.start()
    watcher.start()
    watcher.stop(timeout_seconds=1.0)
    watcher.stop(timeout_seconds=1.0)


def test_cleanup_watcher_run_loop_logs_polling_failure_and_stops() -> None:
    watcher = _watcher(
        ExecutionRegistry(),
        JobStateTransitionCoordinator(),
        cleanup_interval_seconds=0.01,
    )

    def fail_and_stop() -> None:
        watcher._stop_requested.set()  # pylint: disable=protected-access
        raise RuntimeError("poll failed")

    poll_once_mock = MagicMock(side_effect=fail_and_stop)
    setattr(watcher, "poll_once", poll_once_mock)

    watcher._run_loop()  # pylint: disable=protected-access

    poll_once_mock.assert_called_once()


def test_cleanup_watcher_deletes_terminal_workspace_when_record_has_no_context(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    coordinator = _coordinator_in_state(WorkerJobStatus.ERROR)

    record = _record(tmp_path)
    record.context = None

    record.workspace.work_dir.mkdir(parents=True)
    (record.workspace.work_dir / "partial-input.csv").write_text(
        "partial data",
        encoding="utf-8",
    )

    registry.add(record)

    watcher = _watcher(registry, coordinator)

    watcher.poll_once()

    assert not record.workspace.work_dir.exists()
    assert registry.get(record.job_id) is None
    assert coordinator.get_state(record.job_id) is None


def _watcher(
    execution_registry: Any,
    job_state_transition_coordinator: JobStateTransitionCoordinator,
    *,
    cleanup_interval_seconds: float = 0.01,
) -> CleanupWatcher:
    return CleanupWatcher(
        execution_registry=execution_registry,
        job_state_transition_coordinator=job_state_transition_coordinator,
        worker_id=WORKER_ID,
        cleanup_interval_seconds=cleanup_interval_seconds,
    )


def _coordinator_in_state(
    state: WorkerJobStatus = WorkerJobStatus.SUBMITTED,
    job_id: str = JOB_ID,
) -> JobStateTransitionCoordinator:
    coordinator = JobStateTransitionCoordinator()
    acknowledger = cast(Acknowledger, cast(object, MagicMock(spec=Acknowledger)))
    record = coordinator.create(job_id=job_id, submitted_ack=acknowledger)
    with record.lock:
        record.state = state
    return coordinator


def _record(
    tmp_path: Path,
    *,
    job_id: str = JOB_ID,
    work_dir: Path | None = None,
) -> ExecutionRecord:
    context = _context(tmp_path, job_id=job_id, work_dir=work_dir)

    return ExecutionRecord(
        workspace=context.workspace,
        context=context,
    )


def _context(
    tmp_path: Path,
    *,
    job_id: str = JOB_ID,
    work_dir: Path | None = None,
) -> JobExecutionContext:
    resolved_work_dir = work_dir or tmp_path / "jobs" / "42" / job_id
    input_dir = resolved_work_dir / "in"
    output_dir = resolved_work_dir / "out"

    manifest = JobManifest(
        manifest_version=1,
        user_id=42,
        job_id=job_id,
        job_type="SOLVING_SLAE",
        inputs={
            "matrix": ArtifactRef(
                object_key=f"jobs/42/{job_id}/in/matrix.csv",
                format=ArtifactFormat.CSV,
            ),
            "rhs": ArtifactRef(
                object_key=f"jobs/42/{job_id}/in/rhs.csv",
                format=ArtifactFormat.CSV,
            ),
        },
        params={
            "solvingMethod": "numpy_exact_solver",
        },
        outputs={
            "solution": ArtifactRef(
                object_key=f"jobs/42/{job_id}/out/solution.csv",
                format=ArtifactFormat.CSV,
            ),
        },
    )

    workspace = JobWorkspace(
        manifest=manifest,
        work_dir=resolved_work_dir,
        input_dir=input_dir,
        output_dir=output_dir,
        worker_id=WORKER_ID,
    )

    return JobExecutionContext(
        workspace=workspace,
        inputs=InputArtifacts(
            {
                "matrix": PreparedInputArtifact(
                    object_key=f"jobs/42/{job_id}/in/matrix.csv",
                    local_path=input_dir / "matrix.csv",
                    format=ArtifactFormat.CSV,
                ),
                "rhs": PreparedInputArtifact(
                    object_key=f"jobs/42/{job_id}/in/rhs.csv",
                    local_path=input_dir / "rhs.csv",
                    format=ArtifactFormat.CSV,
                ),
            }
        ),
        outputs=OutputArtifacts(
            {
                "solution": PreparedOutputArtifact(
                    object_key=f"jobs/42/{job_id}/out/solution.csv",
                    local_path=output_dir / "solution.csv",
                    format=ArtifactFormat.CSV,
                ),
            }
        ),
        params=JobParameters(manifest.params),
    )


def _create_work_dir(record: ExecutionRecord) -> None:
    work_dir = record.context.work_dir
    work_dir.mkdir(parents=True)
    (work_dir / "runtime-file.txt").write_text(
        "runtime data",
        encoding="utf-8",
    )

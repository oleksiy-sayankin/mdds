# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
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

FIXED_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
WORKER_ID = "worker-1"


@pytest.mark.parametrize(
    ("field_name", "bad_value", "error_message"),
    [
        ("execution_registry", None, "execution_registry cannot be null."),
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
        "worker_id": WORKER_ID,
        "cleanup_interval_seconds": 0.01,
    }
    kwargs[field_name] = bad_value

    with pytest.raises(ValueError) as exc_info:
        CleanupWatcher(**kwargs)

    assert str(exc_info.value) == error_message


def test_cleanup_watcher_ignores_running_record(tmp_path: Path) -> None:
    registry = ExecutionRegistry()
    record = _record(tmp_path)
    _create_work_dir(record)
    registry.add(record)

    watcher = _watcher(registry)

    watcher.poll_once()

    assert record.context.work_dir.exists()
    assert registry.get("job-1") is record
    assert registry.size() == 1


def test_cleanup_watcher_ignores_terminal_claimed_but_not_published_record(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    record = _record(
        tmp_path,
        terminal_status_claimed=True,
        terminal_status_published=False,
        acknowledgement_done=False,
        cleanup_ready=False,
    )
    _create_work_dir(record)
    registry.add(record)

    watcher = _watcher(registry)

    watcher.poll_once()

    assert record.context.work_dir.exists()
    assert registry.get("job-1") is record
    assert registry.size() == 1


def test_cleanup_watcher_ignores_terminal_published_but_not_acknowledged_record(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    record = _record(
        tmp_path,
        terminal_status_claimed=True,
        terminal_status_published=True,
        acknowledgement_done=False,
        cleanup_ready=False,
    )
    _create_work_dir(record)
    registry.add(record)

    watcher = _watcher(registry)

    watcher.poll_once()

    assert record.context.work_dir.exists()
    assert registry.get("job-1") is record
    assert registry.size() == 1


def test_cleanup_watcher_ignores_acknowledged_but_not_cleanup_ready_record(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    record = _record(
        tmp_path,
        terminal_status_claimed=True,
        terminal_status_published=True,
        acknowledgement_done=True,
        cleanup_ready=False,
    )
    _create_work_dir(record)
    registry.add(record)

    watcher = _watcher(registry)

    watcher.poll_once()

    assert record.context.work_dir.exists()
    assert registry.get("job-1") is record
    assert registry.size() == 1


def test_cleanup_watcher_deletes_work_dir_and_removes_cleanup_ready_record(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    record = _cleanup_ready_record(tmp_path)
    _create_work_dir(record)
    registry.add(record)

    watcher = _watcher(registry)

    watcher.poll_once()

    assert not record.context.work_dir.exists()
    assert registry.get("job-1") is None
    assert registry.size() == 0


def test_cleanup_watcher_removes_record_when_work_dir_is_already_absent(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    record = _cleanup_ready_record(tmp_path)
    registry.add(record)

    watcher = _watcher(registry)

    watcher.poll_once()

    assert not record.context.work_dir.exists()
    assert registry.get("job-1") is None
    assert registry.size() == 0


def test_cleanup_watcher_keeps_record_when_work_dir_is_file_not_directory(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    work_dir = tmp_path / "jobs" / "42" / "job-1"
    work_dir.parent.mkdir(parents=True)
    work_dir.write_text("not a directory", encoding="utf-8")

    record = _cleanup_ready_record(tmp_path, work_dir=work_dir)
    registry.add(record)

    watcher = _watcher(registry)

    watcher.poll_once()

    assert work_dir.exists()
    assert work_dir.is_file()
    assert registry.get("job-1") is record
    assert registry.size() == 1


def test_cleanup_watcher_keeps_record_when_rmtree_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = ExecutionRegistry()
    record = _cleanup_ready_record(tmp_path)
    _create_work_dir(record)
    registry.add(record)

    def fail_rmtree(_path: Path) -> None:
        raise RuntimeError("delete failed")

    monkeypatch.setattr(
        "mdds_worker_runtime.execution.cleanup_watcher.shutil.rmtree",
        fail_rmtree,
    )

    watcher = _watcher(registry)

    watcher.poll_once()

    assert record.context.work_dir.exists()
    assert registry.get("job-1") is record
    assert registry.size() == 1


def test_cleanup_watcher_remove_if_same_false_does_not_fail(tmp_path: Path) -> None:
    record = _cleanup_ready_record(tmp_path)
    _create_work_dir(record)

    registry = MagicMock()
    registry.snapshot.return_value = [record]
    registry.remove_if_same.return_value = False

    watcher = _watcher(registry)

    watcher.poll_once()

    assert not record.context.work_dir.exists()
    registry.remove_if_same.assert_called_once_with(record)


def test_cleanup_watcher_ignores_none_record() -> None:
    registry = MagicMock()
    registry.snapshot.return_value = [None]

    watcher = _watcher(registry)

    watcher.poll_once()

    registry.remove_if_same.assert_not_called()


def test_cleanup_watcher_start_stop_are_idempotent() -> None:
    watcher = _watcher(
        ExecutionRegistry(),
        cleanup_interval_seconds=0.01,
    )

    watcher.start()
    watcher.start()
    watcher.stop(timeout_seconds=1.0)
    watcher.stop(timeout_seconds=1.0)


def test_cleanup_watcher_run_loop_logs_polling_failure_and_stops() -> None:
    watcher = _watcher(
        ExecutionRegistry(),
        cleanup_interval_seconds=0.01,
    )

    def fail_and_stop() -> None:
        watcher._stop_requested.set()  # pylint: disable=protected-access
        raise RuntimeError("poll failed")

    poll_once_mock = MagicMock(side_effect=fail_and_stop)
    setattr(watcher, "poll_once", poll_once_mock)

    watcher._run_loop()  # pylint: disable=protected-access

    poll_once_mock.assert_called_once()


def _watcher(
    execution_registry: Any,
    *,
    cleanup_interval_seconds: float = 0.01,
) -> CleanupWatcher:
    return CleanupWatcher(
        execution_registry=execution_registry,
        worker_id=WORKER_ID,
        cleanup_interval_seconds=cleanup_interval_seconds,
    )


def _cleanup_ready_record(
    tmp_path: Path,
    *,
    job_id: str = "job-1",
    work_dir: Path | None = None,
) -> ExecutionRecord:
    return _record(
        tmp_path,
        job_id=job_id,
        work_dir=work_dir,
        terminal_status_claimed=True,
        terminal_status_published=True,
        acknowledgement_done=True,
        cleanup_ready=True,
    )


def _record(
    tmp_path: Path,
    *,
    job_id: str = "job-1",
    work_dir: Path | None = None,
    terminal_status_claimed: bool = False,
    terminal_status_published: bool = False,
    acknowledgement_done: bool = False,
    cleanup_ready: bool = False,
) -> ExecutionRecord:
    context = _context(tmp_path, job_id=job_id, work_dir=work_dir)

    record = ExecutionRecord(
        job_id=job_id,
        user_id=context.user_id,
        job_type=context.job_type,
        worker_id=WORKER_ID,
        manifest_object_key=f"jobs/{context.user_id}/{job_id}/manifest.json",
        manifest=MagicMock(),
        context=context,
        process=MagicMock(),
        parent_connection=MagicMock(),
        submitted_ack=MagicMock(),
        started_at=FIXED_TIME,
    )

    record.terminal_status_claimed = terminal_status_claimed
    record.terminal_status = WorkerJobStatus.DONE if terminal_status_claimed else None
    record.terminal_message = (
        "Job completed successfully." if terminal_status_claimed else None
    )
    record.finished_at = FIXED_TIME if terminal_status_claimed else None
    record.terminal_status_published = terminal_status_published
    record.acknowledgement_done = acknowledgement_done
    record.cleanup_ready = cleanup_ready

    return record


def _context(
    tmp_path: Path,
    *,
    job_id: str = "job-1",
    work_dir: Path | None = None,
) -> JobExecutionContext:
    resolved_work_dir = work_dir or tmp_path / "jobs" / "42" / job_id
    input_dir = resolved_work_dir / "in"
    output_dir = resolved_work_dir / "out"

    return JobExecutionContext(
        user_id=42,
        job_id=job_id,
        job_type="SOLVING_SLAE",
        work_dir=resolved_work_dir,
        input_dir=input_dir,
        output_dir=output_dir,
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
        params=JobParameters(
            {
                "solvingMethod": "numpy_exact_solver",
            }
        ),
    )


def _create_work_dir(record: ExecutionRecord) -> None:
    record.context.work_dir.mkdir(parents=True)
    (record.context.work_dir / "runtime-file.txt").write_text(
        "runtime data",
        encoding="utf-8",
    )

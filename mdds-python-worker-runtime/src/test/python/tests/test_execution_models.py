# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

from mdds_worker_runtime.domain.manifest import JobManifest
from mdds_worker_runtime.execution.models import (
    ExecutionRecord,
    WorkerJobStatus,
    ProcessRecord,
)
from mdds_worker_runtime.execution.workspace import JobWorkspace

FIXED_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_worker_job_status_values_are_stable() -> None:
    assert WorkerJobStatus.IN_PROGRESS.value == "IN_PROGRESS"
    assert WorkerJobStatus.DONE.value == "DONE"
    assert WorkerJobStatus.ERROR.value == "ERROR"
    assert WorkerJobStatus.CANCELLED.value == "CANCELLED"


def test_execution_record_defaults_represent_active_non_terminal_execution() -> None:
    record = _record()

    assert record.job_id == "job-1"
    assert record.user_id == 42
    assert record.job_type == "SOLVING_SLAE"
    assert record.worker_id == "worker-1"
    assert record.started_at == FIXED_TIME

    assert record.finished_at is None


def _record(job_id: str = "job-1") -> ExecutionRecord:
    manifest = _manifest(job_id)
    work_dir = Path("jobs") / str(manifest.user_id) / manifest.job_id

    workspace = JobWorkspace(
        manifest=manifest,
        work_dir=work_dir,
        input_dir=work_dir / "in",
        output_dir=work_dir / "out",
        worker_id="worker-1",
    )

    return ExecutionRecord(
        workspace=workspace,
        context=MagicMock(),
        process_record=ProcessRecord(
            process=MagicMock(),
            parent_connection=MagicMock(),
            started_at=FIXED_TIME,
        ),
    )


def _manifest(job_id: str) -> JobManifest:
    return JobManifest(
        manifest_version=1,
        user_id=42,
        job_id=job_id,
        job_type="SOLVING_SLAE",
        inputs={},
        params={},
        outputs={},
    )

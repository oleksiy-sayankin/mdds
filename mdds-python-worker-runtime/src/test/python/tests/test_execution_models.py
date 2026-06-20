# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from mdds_worker_runtime.domain.manifest import JobManifest
from mdds_worker_runtime.execution.models import ExecutionRecord, WorkerJobStatus

FIXED_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_worker_job_status_values_are_stable() -> None:
    assert WorkerJobStatus.VALIDATION_FAILED.value == "VALIDATION_FAILED"
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
    assert record.manifest_object_key == "jobs/42/job-1/manifest.json"
    assert record.started_at == FIXED_TIME

    assert record.finished_at is None
    assert record.terminal_status is None
    assert record.terminal_message is None

    assert record.terminal_status_claimed is False
    assert record.terminal_status_published is False
    assert record.acknowledgement_done is False
    assert record.cleanup_ready is False

    assert record.lock is not None


def test_execution_record_lock_is_not_shared_between_records() -> None:
    first = _record(job_id="job-1")
    second = _record(job_id="job-2")

    assert first.lock is not second.lock


def _record(job_id: str = "job-1") -> ExecutionRecord:
    return ExecutionRecord(
        job_id=job_id,
        user_id=42,
        job_type="SOLVING_SLAE",
        worker_id="worker-1",
        manifest_object_key=f"jobs/42/{job_id}/manifest.json",
        manifest=_manifest(job_id),
        process=MagicMock(),
        parent_connection=MagicMock(),
        submitted_ack=MagicMock(),
        started_at=FIXED_TIME,
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

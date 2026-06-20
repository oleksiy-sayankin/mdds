# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import threading
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.domain.manifest import JobManifest
from mdds_worker_runtime.execution.models import ExecutionRecord, WorkerJobStatus
from mdds_worker_runtime.execution.registry import (
    DuplicateExecutionRecordError,
    ExecutionRecordNotFoundError,
    ExecutionRegistry,
)

FIXED_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
FINISHED_TIME = datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc)


def test_add_stores_record_by_record_job_id() -> None:
    registry = ExecutionRegistry()
    record = _record("job-1")

    registry.add(record)

    assert registry.get("job-1") is record
    assert registry.size() == 1


def test_add_rejects_duplicate_job_id() -> None:
    registry = ExecutionRegistry()

    registry.add(_record("job-1"))

    with pytest.raises(DuplicateExecutionRecordError, match="already exists"):
        registry.add(_record("job-1"))


def test_add_rejects_null_record() -> None:
    registry = ExecutionRegistry()

    with pytest.raises(ValueError, match="record cannot be null"):
        registry.add(None)


def test_add_rejects_blank_job_id() -> None:
    registry = ExecutionRegistry()

    with pytest.raises(ValueError, match="job_id cannot be null or blank"):
        registry.add(_record(" "))


def test_get_returns_none_for_missing_job() -> None:
    registry = ExecutionRegistry()

    assert registry.get("missing-job") is None


def test_get_rejects_blank_job_id() -> None:
    registry = ExecutionRegistry()

    with pytest.raises(ValueError, match="job_id cannot be null or blank"):
        registry.get("")


def test_require_returns_existing_record() -> None:
    registry = ExecutionRegistry()
    record = _record("job-1")

    registry.add(record)

    assert registry.require("job-1") is record


def test_require_raises_for_missing_record() -> None:
    registry = ExecutionRegistry()

    with pytest.raises(ExecutionRecordNotFoundError, match="was not found"):
        registry.require("job-1")


def test_snapshot_returns_stable_copy() -> None:
    registry = ExecutionRegistry()
    first = _record("job-1")
    second = _record("job-2")

    registry.add(first)
    registry.add(second)

    snapshot = registry.snapshot()

    registry.remove("job-1")

    assert snapshot == [first, second]
    assert registry.snapshot() == [second]


def test_remove_removes_record_by_job_id() -> None:
    registry = ExecutionRegistry()
    record = _record("job-1")

    registry.add(record)

    removed = registry.remove("job-1")

    assert removed is record
    assert registry.get("job-1") is None
    assert registry.size() == 0


def test_remove_returns_none_for_missing_job() -> None:
    registry = ExecutionRegistry()

    assert registry.remove("job-1") is None


def test_remove_if_same_removes_same_record() -> None:
    registry = ExecutionRegistry()
    record = _record("job-1")

    registry.add(record)

    assert registry.remove_if_same(record) is True
    assert registry.get("job-1") is None


def test_remove_if_same_does_not_remove_different_record_with_same_job_id() -> None:
    registry = ExecutionRegistry()
    registered = _record("job-1")
    different = _record("job-1")

    registry.add(registered)

    assert registry.remove_if_same(different) is False
    assert registry.get("job-1") is registered


def test_clear_returns_removed_records_and_empties_registry() -> None:
    registry = ExecutionRegistry()
    first = _record("job-1")
    second = _record("job-2")

    registry.add(first)
    registry.add(second)

    removed = registry.clear()

    assert removed == [first, second]
    assert registry.size() == 0
    assert registry.snapshot() == []


def test_try_claim_terminal_marks_done_and_returns_record() -> None:
    registry = ExecutionRegistry()
    record = _record("job-1")
    registry.add(record)

    claimed = registry.try_claim_terminal(
        job_id="job-1",
        terminal_status=WorkerJobStatus.DONE,
        message="Job completed successfully.",
        finished_at=FINISHED_TIME,
    )

    assert claimed is record

    with record.lock:
        assert record.terminal_status_claimed is True
        assert record.terminal_status is WorkerJobStatus.DONE
        assert record.terminal_message == "Job completed successfully."
        assert record.finished_at == FINISHED_TIME


def test_try_claim_terminal_returns_none_for_missing_job() -> None:
    registry = ExecutionRegistry()

    claimed = registry.try_claim_terminal(
        job_id="missing-job",
        terminal_status=WorkerJobStatus.DONE,
        message="Job completed successfully.",
        finished_at=FINISHED_TIME,
    )

    assert claimed is None


def test_try_claim_terminal_rejects_non_terminal_status() -> None:
    registry = ExecutionRegistry()
    record = _record("job-1")
    registry.add(record)

    with pytest.raises(ValueError, match="not terminal"):
        registry.try_claim_terminal(
            job_id="job-1",
            terminal_status=WorkerJobStatus.IN_PROGRESS,
            message="Job is in progress.",
            finished_at=FINISHED_TIME,
        )

    assert record.terminal_status_claimed is False
    assert record.terminal_status is None


def test_try_claim_terminal_wins_only_once() -> None:
    registry = ExecutionRegistry()
    record = _record("job-1")
    registry.add(record)

    first = registry.try_claim_terminal(
        job_id="job-1",
        terminal_status=WorkerJobStatus.DONE,
        message="Job completed successfully.",
        finished_at=FINISHED_TIME,
    )
    second = registry.try_claim_terminal(
        job_id="job-1",
        terminal_status=WorkerJobStatus.ERROR,
        message="Late error.",
        finished_at=FINISHED_TIME,
    )

    assert first is record
    assert second is None

    with record.lock:
        assert record.terminal_status is WorkerJobStatus.DONE
        assert record.terminal_message == "Job completed successfully."


def test_try_claim_terminal_is_thread_safe_and_allows_single_winner() -> None:
    registry = ExecutionRegistry()
    record = _record("job-1")
    registry.add(record)

    barrier = threading.Barrier(4)
    results = []
    results_lock = threading.Lock()

    def claim(status: WorkerJobStatus) -> None:
        barrier.wait()
        result = registry.try_claim_terminal(
            job_id="job-1",
            terminal_status=status,
            message=f"Terminal status: {status.value}",
            finished_at=FINISHED_TIME,
        )
        with results_lock:
            results.append(result)

    threads = [
        threading.Thread(target=claim, args=(WorkerJobStatus.DONE,)),
        threading.Thread(target=claim, args=(WorkerJobStatus.ERROR,)),
        threading.Thread(target=claim, args=(WorkerJobStatus.CANCELLED,)),
        threading.Thread(target=claim, args=(WorkerJobStatus.VALIDATION_FAILED,)),
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join(timeout=5)

    winners = [result for result in results if result is not None]

    assert winners == [record]
    assert len(results) == 4

    with record.lock:
        assert record.terminal_status_claimed is True
        assert record.terminal_status in {
            WorkerJobStatus.DONE,
            WorkerJobStatus.ERROR,
            WorkerJobStatus.CANCELLED,
            WorkerJobStatus.VALIDATION_FAILED,
        }


def test_mark_terminal_published_requires_terminal_status_claimed() -> None:
    registry = ExecutionRegistry()
    record = _record("job-1")
    registry.add(record)

    with pytest.raises(RuntimeError, match="terminal status was not claimed"):
        registry.mark_terminal_published("job-1")

    assert record.terminal_status_published is False


def test_mark_terminal_published_sets_flag_after_terminal_claim() -> None:
    registry = ExecutionRegistry()
    record = _terminal_record(registry)

    registry.mark_terminal_published("job-1")

    assert record.terminal_status_published is True


def test_mark_acknowledged_requires_terminal_status_published() -> None:
    registry = ExecutionRegistry()
    record = _terminal_record(registry)

    with pytest.raises(RuntimeError, match="before terminal status is published"):
        registry.mark_acknowledged("job-1")

    assert record.acknowledgement_done is False


def test_mark_acknowledged_sets_flag_after_terminal_status_publication() -> None:
    registry = ExecutionRegistry()
    record = _terminal_record(registry)

    registry.mark_terminal_published("job-1")
    registry.mark_acknowledged("job-1")

    assert record.acknowledgement_done is True


def test_mark_cleanup_ready_requires_terminal_status_published() -> None:
    registry = ExecutionRegistry()
    record = _terminal_record(registry)

    with pytest.raises(RuntimeError, match="before terminal status is published"):
        registry.mark_cleanup_ready("job-1")

    assert record.cleanup_ready is False


def test_mark_cleanup_ready_requires_acknowledgement() -> None:
    registry = ExecutionRegistry()
    record = _terminal_record(registry)

    registry.mark_terminal_published("job-1")

    with pytest.raises(RuntimeError, match="before acknowledgement"):
        registry.mark_cleanup_ready("job-1")

    assert record.cleanup_ready is False


def test_mark_cleanup_ready_sets_flag_after_terminal_publication_and_ack() -> None:
    registry = ExecutionRegistry()
    record = _terminal_record(registry)

    registry.mark_terminal_published("job-1")
    registry.mark_acknowledged("job-1")
    registry.mark_cleanup_ready("job-1")

    assert record.cleanup_ready is True


def test_successful_registry_lifecycle() -> None:
    registry = ExecutionRegistry()
    record = _record("job-1")

    registry.add(record)

    assert registry.get("job-1") is record

    claimed = registry.try_claim_terminal(
        job_id="job-1",
        terminal_status=WorkerJobStatus.DONE,
        message="Job completed successfully.",
        finished_at=FINISHED_TIME,
    )

    assert claimed is record

    registry.mark_terminal_published("job-1")
    registry.mark_acknowledged("job-1")
    registry.mark_cleanup_ready("job-1")

    with record.lock:
        assert record.terminal_status_claimed is True
        assert record.terminal_status_published is True
        assert record.acknowledgement_done is True
        assert record.cleanup_ready is True


def test_mark_methods_raise_for_missing_record() -> None:
    registry = ExecutionRegistry()

    with pytest.raises(ExecutionRecordNotFoundError):
        registry.mark_terminal_published("missing-job")

    with pytest.raises(ExecutionRecordNotFoundError):
        registry.mark_acknowledged("missing-job")

    with pytest.raises(ExecutionRecordNotFoundError):
        registry.mark_cleanup_ready("missing-job")


def _terminal_record(registry: ExecutionRegistry) -> ExecutionRecord:
    record = _record("job-1")
    registry.add(record)
    registry.try_claim_terminal(
        job_id="job-1",
        terminal_status=WorkerJobStatus.DONE,
        message="Job completed successfully.",
        finished_at=FINISHED_TIME,
    )
    return record


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

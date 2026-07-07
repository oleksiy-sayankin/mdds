# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.domain.manifest import JobManifest
from mdds_worker_runtime.execution.models import ExecutionRecord
from mdds_worker_runtime.execution.registry import (
    DuplicateExecutionRecordError,
    ExecutionRecordNotFoundError,
    ExecutionRegistry,
)
from mdds_worker_runtime.execution.workspace import JobWorkspace

FIXED_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


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


def test_remove_rejects_blank_job_id() -> None:
    registry = ExecutionRegistry()

    with pytest.raises(ValueError, match="job_id cannot be null or blank"):
        registry.remove("")


def test_remove_if_same_removes_same_record() -> None:
    registry = ExecutionRegistry()
    record = _record("job-1")

    registry.add(record)

    assert registry.remove_if_same(record) is True
    assert registry.get("job-1") is None
    assert registry.size() == 0


def test_remove_if_same_does_not_remove_different_record_with_same_job_id() -> None:
    registry = ExecutionRegistry()
    registered = _record("job-1")
    different = _record("job-1")

    registry.add(registered)

    assert registry.remove_if_same(different) is False
    assert registry.get("job-1") is registered
    assert registry.size() == 1


def test_remove_if_same_returns_false_for_missing_record() -> None:
    registry = ExecutionRegistry()
    record = _record("job-1")

    assert registry.remove_if_same(record) is False
    assert registry.size() == 0


def test_remove_if_same_rejects_null_record() -> None:
    registry = ExecutionRegistry()

    with pytest.raises(ValueError, match="record cannot be null"):
        registry.remove_if_same(None)


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
    assert registry.get("job-1") is None
    assert registry.get("job-2") is None


def test_clear_returns_empty_list_when_registry_is_empty() -> None:
    registry = ExecutionRegistry()

    assert registry.clear() == []
    assert registry.size() == 0


def test_size_returns_number_of_registered_records() -> None:
    registry = ExecutionRegistry()

    assert registry.size() == 0

    registry.add(_record("job-1"))

    assert registry.size() == 1

    registry.add(_record("job-2"))

    assert registry.size() == 2


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

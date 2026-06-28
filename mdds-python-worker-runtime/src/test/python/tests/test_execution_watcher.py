# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.execution_watcher import ExecutionWatcher
from mdds_worker_runtime.execution.models import ExecutionRecord, WorkerJobStatus
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.execution.supervised_process import (
    SupervisedExecutionResult,
    SupervisedExecutionStatus,
)

FIXED_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
WORKER_ID = "worker-1"
JOB_TIMEOUT_SECONDS = 100.0
PROGRESS_INTERVAL_SECONDS = 5.0


def test_execution_watcher_ignores_running_job_before_progress_interval_elapsed(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=4))
    registry = ExecutionRegistry()
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(poll_result=False),
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    output_artifact_uploader = MagicMock()
    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_in_progress.assert_not_called()
    status_publisher.publish_done.assert_not_called()
    status_publisher.publish_error.assert_not_called()
    output_artifact_uploader.upload.assert_not_called()
    fixture.submitted_ack.ack.assert_not_called()

    assert record.terminal_status_claimed is False
    assert record.terminal_status_published is False
    assert record.acknowledgement_done is False
    assert record.cleanup_ready is False


def test_execution_watcher_publishes_in_progress_when_progress_interval_elapsed(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=5))
    registry = ExecutionRegistry()
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(poll_result=False),
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    output_artifact_uploader = MagicMock()
    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_in_progress.assert_called_once()
    kwargs = status_publisher.publish_in_progress.call_args.kwargs

    assert kwargs["user_id"] == 42
    assert kwargs["job_id"] == "job-1"
    assert kwargs["job_type"] == "SOLVING_SLAE"
    assert kwargs["worker_id"] == WORKER_ID
    assert kwargs["progress"] == 5
    assert "in progress" in kwargs["message"]

    status_publisher.publish_done.assert_not_called()
    status_publisher.publish_error.assert_not_called()
    output_artifact_uploader.upload.assert_not_called()
    fixture.submitted_ack.ack.assert_not_called()


def test_execution_watcher_in_progress_is_at_least_one(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=5))
    registry = ExecutionRegistry()
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(poll_result=False),
        started_at=FIXED_TIME,
    )
    registry.add(fixture.record)

    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        MagicMock(),
        status_publisher,
        clock,
        job_timeout_seconds=1000.0,
    )

    watcher.poll_once()

    kwargs = status_publisher.publish_in_progress.call_args.kwargs

    assert kwargs["progress"] == 1


def test_execution_watcher_in_progress_is_capped_at_ninety_nine(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=1000))
    registry = ExecutionRegistry()
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(poll_result=False),
        started_at=FIXED_TIME,
    )
    registry.add(fixture.record)

    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        MagicMock(),
        status_publisher,
        clock,
        job_timeout_seconds=100.0,
    )

    watcher.poll_once()

    kwargs = status_publisher.publish_in_progress.call_args.kwargs

    assert kwargs["progress"] == 99


def test_execution_watcher_does_not_publish_progress_twice_before_interval_elapsed(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=5))
    registry = ExecutionRegistry()
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(poll_result=False),
        started_at=FIXED_TIME,
    )
    registry.add(fixture.record)

    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        MagicMock(),
        status_publisher,
        clock,
    )

    watcher.poll_once()
    clock.advance(seconds=4)
    watcher.poll_once()

    assert status_publisher.publish_in_progress.call_count == 1


def test_execution_watcher_publishes_progress_again_after_interval_elapsed(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=5))
    registry = ExecutionRegistry()
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(poll_result=False),
        started_at=FIXED_TIME,
    )
    registry.add(fixture.record)

    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        MagicMock(),
        status_publisher,
        clock,
    )

    watcher.poll_once()
    clock.advance(seconds=5)
    watcher.poll_once()

    assert status_publisher.publish_in_progress.call_count == 2

    first_progress = status_publisher.publish_in_progress.call_args_list[0].kwargs[
        "progress"
    ]
    second_progress = status_publisher.publish_in_progress.call_args_list[1].kwargs[
        "progress"
    ]

    assert first_progress == 5
    assert second_progress == 10


def test_execution_watcher_success_uploads_outputs_publishes_done_acks_and_marks_cleanup_ready(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult.succeeded("job-1")
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(result=result),
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    events: list[str] = []
    output_artifact_uploader = MagicMock()
    status_publisher = MagicMock()

    output_artifact_uploader.upload.side_effect = lambda context: events.append(
        "upload"
    )
    status_publisher.publish_done.side_effect = lambda **_kwargs: events.append(
        "publish_done"
    )
    fixture.submitted_ack.ack.side_effect = lambda: events.append("ack")

    watcher = _watcher(
        registry,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    assert events == ["upload", "publish_done", "ack"]

    output_artifact_uploader.upload.assert_called_once_with(record.context)
    status_publisher.publish_done.assert_called_once_with(
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        worker_id=WORKER_ID,
        message="Execution succeeded.",
    )
    status_publisher.publish_error.assert_not_called()
    status_publisher.publish_in_progress.assert_not_called()

    fixture.submitted_ack.ack.assert_called_once()

    assert record.terminal_status_claimed is True
    assert record.terminal_status == WorkerJobStatus.DONE
    assert record.terminal_message == "Execution succeeded."
    assert record.terminal_status_published is True
    assert record.acknowledgement_done is True
    assert record.cleanup_ready is True
    assert record.finished_at == clock.now


def test_execution_watcher_failed_result_publishes_error_acks_and_marks_cleanup_ready(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult(
        job_id="job-1",
        status=SupervisedExecutionStatus.FAILED,
        message="execute failed",
        error_type="RuntimeError",
    )
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(result=result),
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    output_artifact_uploader = MagicMock()
    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    output_artifact_uploader.upload.assert_not_called()
    status_publisher.publish_done.assert_not_called()
    status_publisher.publish_in_progress.assert_not_called()
    status_publisher.publish_error.assert_called_once_with(
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        worker_id=WORKER_ID,
        message="RuntimeError: execute failed",
    )

    fixture.submitted_ack.ack.assert_called_once()

    assert record.terminal_status_claimed is True
    assert record.terminal_status == WorkerJobStatus.ERROR
    assert record.terminal_message == "RuntimeError: execute failed"
    assert record.terminal_status_published is True
    assert record.acknowledgement_done is True
    assert record.cleanup_ready is True
    assert record.finished_at == clock.now


def test_execution_watcher_output_upload_failure_publishes_error_not_done(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult.succeeded("job-1")
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(result=result),
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    output_artifact_uploader = MagicMock()
    output_artifact_uploader.upload.side_effect = RuntimeError("s3 upload failed")
    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    output_artifact_uploader.upload.assert_called_once_with(record.context)
    status_publisher.publish_done.assert_not_called()
    status_publisher.publish_error.assert_called_once()

    kwargs = status_publisher.publish_error.call_args.kwargs
    assert kwargs["user_id"] == 42
    assert kwargs["job_id"] == "job-1"
    assert kwargs["job_type"] == "SOLVING_SLAE"
    assert kwargs["worker_id"] == WORKER_ID
    assert "Output artifact upload failed" in kwargs["message"]
    assert "s3 upload failed" in kwargs["message"]

    fixture.submitted_ack.ack.assert_called_once()

    assert record.terminal_status_claimed is True
    assert record.terminal_status == WorkerJobStatus.ERROR
    assert record.terminal_status_published is True
    assert record.acknowledgement_done is True
    assert record.cleanup_ready is True


def test_execution_watcher_already_terminal_claimed_record_is_ignored(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult.succeeded("job-1")
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(result=result),
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    registry.try_claim_terminal(
        job_id="job-1",
        terminal_status=WorkerJobStatus.ERROR,
        message="Already claimed.",
        finished_at=FIXED_TIME,
    )

    output_artifact_uploader = MagicMock()
    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    output_artifact_uploader.upload.assert_not_called()
    status_publisher.publish_in_progress.assert_not_called()
    status_publisher.publish_done.assert_not_called()
    status_publisher.publish_error.assert_not_called()
    fixture.submitted_ack.ack.assert_not_called()

    assert record.terminal_status == WorkerJobStatus.ERROR
    assert record.terminal_message == "Already claimed."
    assert record.terminal_status_published is False
    assert record.acknowledgement_done is False
    assert record.cleanup_ready is False


def test_execution_watcher_publish_done_failure_does_not_ack_or_mark_cleanup_ready(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult.succeeded("job-1")
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(result=result),
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    output_artifact_uploader = MagicMock()
    status_publisher = MagicMock()
    status_publisher.publish_done.side_effect = RuntimeError("status queue failed")

    watcher = _watcher(
        registry,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    with pytest.raises(RuntimeError, match="status queue failed"):
        watcher.poll_once()

    output_artifact_uploader.upload.assert_called_once_with(record.context)
    status_publisher.publish_done.assert_called_once()
    status_publisher.publish_error.assert_not_called()
    fixture.submitted_ack.ack.assert_not_called()

    assert record.terminal_status_claimed is True
    assert record.terminal_status == WorkerJobStatus.DONE
    assert record.terminal_status_published is False
    assert record.acknowledgement_done is False
    assert record.cleanup_ready is False


def test_execution_watcher_publish_error_failure_does_not_ack_or_mark_cleanup_ready(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult(
        job_id="job-1",
        status=SupervisedExecutionStatus.FAILED,
        message="execute failed",
        error_type="RuntimeError",
    )
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(result=result),
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    status_publisher = MagicMock()
    status_publisher.publish_error.side_effect = RuntimeError("status queue failed")

    watcher = _watcher(
        registry,
        MagicMock(),
        status_publisher,
        clock,
    )

    with pytest.raises(RuntimeError, match="status queue failed"):
        watcher.poll_once()

    status_publisher.publish_error.assert_called_once()
    fixture.submitted_ack.ack.assert_not_called()

    assert record.terminal_status_claimed is True
    assert record.terminal_status == WorkerJobStatus.ERROR
    assert record.terminal_status_published is False
    assert record.acknowledgement_done is False
    assert record.cleanup_ready is False


def test_execution_watcher_unexpected_result_type_is_finalized_as_error(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(result={"status": "SUCCEEDED"}),
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        MagicMock(),
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_error.assert_called_once()
    kwargs = status_publisher.publish_error.call_args.kwargs

    assert kwargs["job_id"] == "job-1"
    assert "UnexpectedSupervisedExecutionResult" in kwargs["message"]
    assert "dict" in kwargs["message"]

    fixture.submitted_ack.ack.assert_called_once()
    assert record.terminal_status == WorkerJobStatus.ERROR
    assert record.cleanup_ready is True


def test_execution_watcher_mismatched_result_job_id_is_finalized_as_error(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult.succeeded("other-job")
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(result=result),
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        MagicMock(),
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_error.assert_called_once()
    kwargs = status_publisher.publish_error.call_args.kwargs

    assert kwargs["job_id"] == "job-1"
    assert "UnexpectedSupervisedExecutionJobId" in kwargs["message"]
    assert "other-job" in kwargs["message"]

    fixture.submitted_ack.ack.assert_called_once()
    assert record.terminal_status == WorkerJobStatus.ERROR
    assert record.cleanup_ready is True


def test_execution_watcher_eof_from_parent_pipe_is_finalized_as_error(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(recv_exception=EOFError()),
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        MagicMock(),
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_error.assert_called_once()
    kwargs = status_publisher.publish_error.call_args.kwargs

    assert kwargs["job_id"] == "job-1"
    assert "EOFError" in kwargs["message"]
    assert "closed its result pipe" in kwargs["message"]

    fixture.submitted_ack.ack.assert_called_once()
    assert record.terminal_status == WorkerJobStatus.ERROR
    assert record.cleanup_ready is True


def test_execution_watcher_start_stop_are_idempotent() -> None:
    clock = _MutableClock(FIXED_TIME)
    registry = ExecutionRegistry()

    watcher = _watcher(
        registry,
        MagicMock(),
        MagicMock(),
        clock,
        poll_interval_seconds=0.01,
        progress_interval_seconds=0.01,
    )

    watcher.start()
    watcher.start()
    watcher.stop(timeout_seconds=1.0)
    watcher.stop(timeout_seconds=1.0)


@pytest.mark.parametrize(
    ("field_name", "bad_value", "error_message"),
    [
        ("execution_registry", None, "execution_registry cannot be null."),
        (
            "output_artifact_uploader",
            None,
            "output_artifact_uploader cannot be null.",
        ),
        ("status_publisher", None, "status_publisher cannot be null."),
        ("worker_id", None, "worker_id cannot be null or blank."),
        ("worker_id", "", "worker_id cannot be null or blank."),
        ("worker_id", " ", "worker_id cannot be null or blank."),
        (
            "job_timeout_seconds",
            0,
            "job_timeout_seconds must be greater than zero.",
        ),
        (
            "poll_interval_seconds",
            0,
            "poll_interval_seconds must be greater than zero.",
        ),
        (
            "progress_interval_seconds",
            0,
            "progress_interval_seconds must be greater than zero.",
        ),
        ("clock", None, "clock cannot be null."),
    ],
)
def test_execution_watcher_rejects_invalid_constructor_arguments(
    field_name: str,
    bad_value,
    error_message: str,
) -> None:
    kwargs = {
        "execution_registry": ExecutionRegistry(),
        "output_artifact_uploader": MagicMock(),
        "status_publisher": MagicMock(),
        "worker_id": WORKER_ID,
        "job_timeout_seconds": JOB_TIMEOUT_SECONDS,
        "poll_interval_seconds": 0.01,
        "progress_interval_seconds": PROGRESS_INTERVAL_SECONDS,
        "clock": _MutableClock(FIXED_TIME),
    }
    kwargs[field_name] = bad_value

    with pytest.raises(ValueError, match=error_message):
        ExecutionWatcher(**kwargs)


def test_execution_watcher_run_loop_logs_polling_failure_and_stops() -> None:
    clock = _MutableClock(FIXED_TIME)
    watcher = _watcher(
        ExecutionRegistry(),
        MagicMock(),
        MagicMock(),
        clock,
        poll_interval_seconds=0.01,
    )

    def fail_and_stop() -> None:
        watcher._stop_requested.set()  # pylint: disable=protected-access
        raise RuntimeError("poll failed")

    poll_once_mock = MagicMock(side_effect=fail_and_stop)
    setattr(watcher, "poll_once", poll_once_mock)

    watcher._run_loop()  # pylint: disable=protected-access

    poll_once_mock.assert_called_once()


def test_execution_watcher_ignores_none_record() -> None:
    clock = _MutableClock(FIXED_TIME)
    registry = MagicMock()
    registry.snapshot.return_value = [None]

    output_artifact_uploader = MagicMock()
    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    output_artifact_uploader.upload.assert_not_called()
    status_publisher.publish_in_progress.assert_not_called()
    status_publisher.publish_done.assert_not_called()
    status_publisher.publish_error.assert_not_called()


def test_execution_watcher_recv_os_error_is_finalized_as_error(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(recv_exception=OSError("broken pipe")),
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        MagicMock(),
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_error.assert_called_once()
    kwargs = status_publisher.publish_error.call_args.kwargs

    assert kwargs["job_id"] == "job-1"
    assert "OSError" in kwargs["message"]
    assert "Cannot read supervised execution result" in kwargs["message"]
    assert "broken pipe" in kwargs["message"]

    fixture.submitted_ack.ack.assert_called_once()
    assert record.terminal_status == WorkerJobStatus.ERROR
    assert record.cleanup_ready is True


def test_execution_watcher_progress_publication_failure_does_not_fail_polling(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=5))
    registry = ExecutionRegistry()
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(poll_result=False),
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    status_publisher = MagicMock()
    status_publisher.publish_in_progress.side_effect = RuntimeError(
        "status queue failed"
    )
    output_artifact_uploader = MagicMock()

    watcher = _watcher(
        registry,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_in_progress.assert_called_once()
    status_publisher.publish_done.assert_not_called()
    status_publisher.publish_error.assert_not_called()
    output_artifact_uploader.upload.assert_not_called()
    fixture.submitted_ack.ack.assert_not_called()

    assert record.terminal_status_claimed is False
    assert record.terminal_status_published is False
    assert record.acknowledgement_done is False
    assert record.cleanup_ready is False


def test_execution_watcher_success_does_not_publish_done_when_terminal_claim_fails(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    result = SupervisedExecutionResult.succeeded("job-1")
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(result=result),
        started_at=FIXED_TIME,
    )
    record = fixture.record

    registry = MagicMock()
    registry.snapshot.return_value = [record]
    registry.try_claim_terminal.return_value = None

    output_artifact_uploader = MagicMock()
    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    output_artifact_uploader.upload.assert_called_once_with(record.context)
    registry.try_claim_terminal.assert_called_once_with(
        job_id="job-1",
        terminal_status=WorkerJobStatus.DONE,
        message="Execution succeeded.",
        finished_at=clock.now,
    )
    status_publisher.publish_done.assert_not_called()
    status_publisher.publish_error.assert_not_called()
    fixture.submitted_ack.ack.assert_not_called()
    registry.mark_terminal_published.assert_not_called()
    registry.mark_acknowledged.assert_not_called()
    registry.mark_cleanup_ready.assert_not_called()


def test_execution_watcher_error_is_not_published_when_terminal_claim_fails(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    result = SupervisedExecutionResult(
        job_id="job-1",
        status=SupervisedExecutionStatus.FAILED,
        message="execute failed",
        error_type="RuntimeError",
    )
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(result=result),
        started_at=FIXED_TIME,
    )
    record = fixture.record

    registry = MagicMock()
    registry.snapshot.return_value = [record]
    registry.try_claim_terminal.return_value = None

    output_artifact_uploader = MagicMock()
    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    output_artifact_uploader.upload.assert_not_called()
    registry.try_claim_terminal.assert_called_once_with(
        job_id="job-1",
        terminal_status=WorkerJobStatus.ERROR,
        message="RuntimeError: execute failed",
        finished_at=clock.now,
    )
    status_publisher.publish_done.assert_not_called()
    status_publisher.publish_error.assert_not_called()
    fixture.submitted_ack.ack.assert_not_called()
    registry.mark_terminal_published.assert_not_called()
    registry.mark_acknowledged.assert_not_called()
    registry.mark_cleanup_ready.assert_not_called()


def test_execution_watcher_unknown_result_status_is_finalized_as_error(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult(
        job_id="job-1",
        status="UNKNOWN",
        message="unexpected status",
        error_type=None,
    )
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(result=result),
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        MagicMock(),
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_error.assert_called_once()
    kwargs = status_publisher.publish_error.call_args.kwargs

    assert kwargs["job_id"] == "job-1"
    assert "Unknown supervised execution status" in kwargs["message"]
    assert "UNKNOWN" in kwargs["message"]

    fixture.submitted_ack.ack.assert_called_once()
    assert record.terminal_status == WorkerJobStatus.ERROR
    assert record.cleanup_ready is True


def test_execution_watcher_failed_result_without_error_type_uses_message_only(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult(
        job_id="job-1",
        status=SupervisedExecutionStatus.FAILED,
        message="execute failed",
        error_type=None,
    )
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(result=result),
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        MagicMock(),
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_error.assert_called_once_with(
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        worker_id=WORKER_ID,
        message="execute failed",
    )

    fixture.submitted_ack.ack.assert_called_once()
    assert record.terminal_status == WorkerJobStatus.ERROR
    assert record.terminal_message == "execute failed"
    assert record.cleanup_ready is True


def _watcher(
    execution_registry: ExecutionRegistry,
    output_artifact_uploader,
    status_publisher,
    clock: "_MutableClock",
    *,
    job_timeout_seconds: float = JOB_TIMEOUT_SECONDS,
    poll_interval_seconds: float = 0.01,
    progress_interval_seconds: float = PROGRESS_INTERVAL_SECONDS,
) -> ExecutionWatcher:
    return ExecutionWatcher(
        execution_registry=execution_registry,
        output_artifact_uploader=output_artifact_uploader,
        status_publisher=status_publisher,
        worker_id=WORKER_ID,
        job_timeout_seconds=job_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        progress_interval_seconds=progress_interval_seconds,
        clock=clock,
    )


def _record(
    tmp_path: Path,
    *,
    job_id: str = "job-1",
    parent_connection: "_FakeParentConnection",
    started_at: datetime,
) -> "_RecordFixture":
    context = _context(tmp_path, job_id=job_id)
    submitted_ack = MagicMock()

    record = ExecutionRecord(
        job_id=job_id,
        user_id=context.user_id,
        job_type=context.job_type,
        worker_id=WORKER_ID,
        manifest_object_key=f"jobs/{context.user_id}/{job_id}/manifest.json",
        manifest=MagicMock(),
        context=context,
        process=MagicMock(),
        parent_connection=parent_connection,
        submitted_ack=submitted_ack,
        started_at=started_at,
    )

    return _RecordFixture(
        record=record,
        submitted_ack=submitted_ack,
    )


def _context(tmp_path: Path, *, job_id: str = "job-1") -> JobExecutionContext:
    work_dir = tmp_path / "jobs" / "42" / job_id
    input_dir = work_dir / "in"
    output_dir = work_dir / "out"

    return JobExecutionContext(
        user_id=42,
        job_id=job_id,
        job_type="SOLVING_SLAE",
        work_dir=work_dir,
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


@dataclass(frozen=True)
class _RecordFixture:
    record: ExecutionRecord
    submitted_ack: MagicMock


class _MutableClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, *, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class _FakeParentConnection:
    def __init__(
        self,
        result: Any = None,
        *,
        poll_result: bool = True,
        poll_exception: BaseException | None = None,
        recv_exception: BaseException | None = None,
    ) -> None:
        self._result = result
        self._poll_result = poll_result
        self._poll_exception = poll_exception
        self._recv_exception = recv_exception

    def poll(self) -> bool:
        if self._poll_exception is not None:
            raise self._poll_exception
        return self._poll_result

    def recv(self) -> Any:
        if self._recv_exception is not None:
            raise self._recv_exception
        return self._result

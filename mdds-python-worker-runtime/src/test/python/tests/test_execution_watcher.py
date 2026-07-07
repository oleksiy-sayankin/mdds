# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.execution_watcher import ExecutionWatcher
from mdds_worker_runtime.execution.models import (
    ExecutionRecord,
    WorkerJobStatus,
    ProcessRecord,
)
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.execution.supervised_process import (
    SupervisedExecutionResult,
    SupervisedExecutionStatus,
)
from mdds_worker_runtime.execution.workspace import JobWorkspace
from mdds_worker_runtime.job_state import (
    JobStateTransitionCoordinator,
)
from mdds_worker_runtime.queue.queue_client import Acknowledger

FIXED_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
WORKER_ID = "worker-1"
JOB_ID = "job-1"
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
    registry.add(fixture.record)

    output_artifact_uploader = MagicMock()
    status_publisher = MagicMock()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_in_progress.assert_not_called()
    status_publisher.publish_done.assert_not_called()
    status_publisher.publish_error.assert_not_called()
    output_artifact_uploader.upload.assert_not_called()
    ack_mock.ack.assert_not_called()

    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.IN_PROGRESS


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
    registry.add(fixture.record)

    output_artifact_uploader = MagicMock()
    status_publisher = MagicMock()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_in_progress.assert_called_once()
    kwargs = status_publisher.publish_in_progress.call_args.kwargs

    assert kwargs["workspace"] is fixture.record.workspace
    assert kwargs["progress"] == 5
    assert "in progress" in kwargs["message"]

    status_publisher.publish_done.assert_not_called()
    status_publisher.publish_error.assert_not_called()
    output_artifact_uploader.upload.assert_not_called()
    ack_mock.ack.assert_not_called()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.IN_PROGRESS


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
    coordinator = _coordinator_in_state(WorkerJobStatus.IN_PROGRESS)

    watcher = _watcher(
        registry,
        coordinator,
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
    coordinator = _coordinator_in_state(WorkerJobStatus.IN_PROGRESS)

    watcher = _watcher(
        registry,
        coordinator,
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
    coordinator = _coordinator_in_state(WorkerJobStatus.IN_PROGRESS)

    watcher = _watcher(
        registry,
        coordinator,
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
    coordinator = _coordinator_in_state(WorkerJobStatus.IN_PROGRESS)

    watcher = _watcher(
        registry,
        coordinator,
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


def test_execution_watcher_success_uploads_outputs_publishes_done_and_acks(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult.succeeded(JOB_ID)
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
    ack_mock = MagicMock(spec=Acknowledger)
    ack_mock.ack.side_effect = lambda context: events.append("ack")
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    output_artifact_uploader.upload.side_effect = lambda context: events.append(
        "upload"
    )
    status_publisher.publish_done.side_effect = lambda **_kwargs: events.append(
        "publish_done"
    )
    ack_mock.ack.side_effect = lambda: events.append("ack")

    watcher = _watcher(
        registry,
        coordinator,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    assert events == ["upload", "publish_done", "ack"]

    output_artifact_uploader.upload.assert_called_once_with(record.context)
    status_publisher.publish_done.assert_called_once_with(
        workspace=record.workspace,
        message="Execution succeeded.",
    )
    status_publisher.publish_error.assert_not_called()
    status_publisher.publish_in_progress.assert_not_called()

    ack_mock.ack.assert_called_once()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.DONE


def test_execution_watcher_failed_result_publishes_error_and_acks(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult(
        job_id=JOB_ID,
        status=SupervisedExecutionStatus.FAILED,
        message="execute failed",
        error_type="RuntimeError",
    )
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(result=result),
        started_at=FIXED_TIME,
    )
    registry.add(fixture.record)

    output_artifact_uploader = MagicMock()
    status_publisher = MagicMock()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    output_artifact_uploader.upload.assert_not_called()
    status_publisher.publish_done.assert_not_called()
    status_publisher.publish_in_progress.assert_not_called()
    status_publisher.publish_error.assert_called_once_with(
        workspace=fixture.record.workspace,
        message="RuntimeError: execute failed",
    )

    ack_mock.ack.assert_called_once()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.ERROR


def test_execution_watcher_output_upload_failure_publishes_error_not_done(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult.succeeded(JOB_ID)
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
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    output_artifact_uploader.upload.assert_called_once_with(record.context)
    status_publisher.publish_done.assert_not_called()
    status_publisher.publish_error.assert_called_once()

    kwargs = status_publisher.publish_error.call_args.kwargs
    assert kwargs["workspace"] is fixture.record.workspace
    assert "Output artifact upload failed" in kwargs["message"]
    assert "s3 upload failed" in kwargs["message"]

    ack_mock.ack.assert_called_once()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.ERROR


def test_execution_watcher_terminal_state_record_is_ignored(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult.succeeded(JOB_ID)
    parent_connection = _FakeParentConnection(result=result)
    fixture = _record(
        tmp_path,
        parent_connection=parent_connection,
        started_at=FIXED_TIME,
    )
    registry.add(fixture.record)

    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack, WorkerJobStatus.ERROR)

    output_artifact_uploader = MagicMock()
    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        coordinator,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    assert parent_connection.close_count == 0
    fixture.process.join.assert_not_called()
    output_artifact_uploader.upload.assert_not_called()
    status_publisher.publish_in_progress.assert_not_called()
    status_publisher.publish_done.assert_not_called()
    status_publisher.publish_error.assert_not_called()
    ack_mock.ack.assert_not_called()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.ERROR


def test_execution_watcher_success_when_state_is_not_running(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult.succeeded(JOB_ID)
    parent_connection = _FakeParentConnection(result=result)
    fixture = _record(
        tmp_path,
        parent_connection=parent_connection,
        started_at=FIXED_TIME,
    )
    registry.add(fixture.record)

    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack, WorkerJobStatus.VALIDATED)

    output_artifact_uploader = MagicMock()
    status_publisher = MagicMock()

    watcher = _watcher(
        registry,
        coordinator,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    assert parent_connection.close_count == 0
    output_artifact_uploader.upload.assert_not_called()
    status_publisher.publish_done.assert_not_called()
    ack_mock.ack.assert_not_called()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.VALIDATED


def test_execution_watcher_publish_done_failure_does_not_ack_or_commit_done(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult.succeeded(JOB_ID)
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
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    output_artifact_uploader.upload.assert_called_once_with(record.context)
    status_publisher.publish_done.assert_called_once()
    status_publisher.publish_error.assert_not_called()
    ack_mock.ack.assert_not_called()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.IN_PROGRESS


def test_execution_watcher_publish_error_failure_does_not_ack_or_commit_error(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult(
        job_id=JOB_ID,
        status=SupervisedExecutionStatus.FAILED,
        message="execute failed",
        error_type="RuntimeError",
    )
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(result=result),
        started_at=FIXED_TIME,
    )
    registry.add(fixture.record)

    status_publisher = MagicMock()
    status_publisher.publish_error.side_effect = RuntimeError("status queue failed")
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
        MagicMock(),
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_error.assert_called_once()
    ack_mock.ack.assert_not_called()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.IN_PROGRESS


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
    registry.add(fixture.record)

    status_publisher = MagicMock()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
        MagicMock(),
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_error.assert_called_once()
    kwargs = status_publisher.publish_error.call_args.kwargs

    assert kwargs["workspace"] is fixture.record.workspace
    assert kwargs["workspace"].job_id == JOB_ID
    assert "UnexpectedSupervisedExecutionResult" in kwargs["message"]
    assert "dict" in kwargs["message"]

    ack_mock.ack.assert_called_once()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.ERROR


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
    registry.add(fixture.record)

    status_publisher = MagicMock()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
        MagicMock(),
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_error.assert_called_once()
    kwargs = status_publisher.publish_error.call_args.kwargs

    assert kwargs["workspace"] is fixture.record.workspace
    assert kwargs["workspace"].job_id == JOB_ID
    assert "UnexpectedSupervisedExecutionJobId" in kwargs["message"]
    assert "other-job" in kwargs["message"]

    ack_mock.ack.assert_called_once()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.ERROR


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
    registry.add(fixture.record)

    status_publisher = MagicMock()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
        MagicMock(),
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_error.assert_called_once()
    kwargs = status_publisher.publish_error.call_args.kwargs

    assert kwargs["workspace"] is fixture.record.workspace
    assert kwargs["workspace"].job_id == JOB_ID
    assert "EOFError" in kwargs["message"]
    assert "closed its result pipe" in kwargs["message"]

    ack_mock.ack.assert_called_once()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.ERROR


def test_execution_watcher_start_stop_are_idempotent() -> None:
    clock = _MutableClock(FIXED_TIME)
    registry = ExecutionRegistry()

    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
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
            "job_state_transition_coordinator",
            None,
            "job_state_transition_coordinator cannot be null.",
        ),
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
        "job_state_transition_coordinator": JobStateTransitionCoordinator(),
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
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        ExecutionRegistry(),
        coordinator,
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
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
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
    registry.add(fixture.record)

    status_publisher = MagicMock()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
        MagicMock(),
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_error.assert_called_once()
    kwargs = status_publisher.publish_error.call_args.kwargs

    assert kwargs["workspace"] is fixture.record.workspace
    assert kwargs["workspace"].job_id == JOB_ID
    assert "OSError" in kwargs["message"]
    assert "Cannot read supervised execution result" in kwargs["message"]
    assert "broken pipe" in kwargs["message"]

    ack_mock.ack.assert_called_once()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.ERROR


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
    registry.add(fixture.record)

    status_publisher = MagicMock()
    status_publisher.publish_in_progress.side_effect = RuntimeError(
        "status queue failed"
    )
    output_artifact_uploader = MagicMock()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_in_progress.assert_called_once()
    status_publisher.publish_done.assert_not_called()
    status_publisher.publish_error.assert_not_called()
    output_artifact_uploader.upload.assert_not_called()
    ack_mock.ack.assert_not_called()

    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.IN_PROGRESS


def test_execution_watcher_unknown_result_status_is_finalized_as_error(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult(
        job_id=JOB_ID,
        status="UNKNOWN",
        message="unexpected status",
        error_type=None,
    )
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(result=result),
        started_at=FIXED_TIME,
    )
    registry.add(fixture.record)

    status_publisher = MagicMock()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
        MagicMock(),
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_error.assert_called_once()
    kwargs = status_publisher.publish_error.call_args.kwargs

    assert kwargs["workspace"] is fixture.record.workspace
    assert kwargs["workspace"].job_id == JOB_ID
    assert "Unknown supervised execution status" in kwargs["message"]
    assert "UNKNOWN" in kwargs["message"]

    ack_mock.ack.assert_called_once()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.ERROR


def test_execution_watcher_failed_result_without_error_type_uses_message_only(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult(
        job_id=JOB_ID,
        status=SupervisedExecutionStatus.FAILED,
        message="execute failed",
        error_type=None,
    )
    fixture = _record(
        tmp_path,
        parent_connection=_FakeParentConnection(result=result),
        started_at=FIXED_TIME,
    )
    registry.add(fixture.record)

    status_publisher = MagicMock()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
        MagicMock(),
        status_publisher,
        clock,
    )

    watcher.poll_once()

    status_publisher.publish_error.assert_called_once_with(
        workspace=fixture.record.workspace,
        message="execute failed",
    )

    ack_mock.ack.assert_called_once()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.ERROR


def test_execution_watcher_does_not_close_resources_when_no_result_is_available(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=4))
    registry = ExecutionRegistry()
    parent_connection = _FakeParentConnection(poll_result=False)
    fixture = _record(
        tmp_path,
        parent_connection=parent_connection,
        started_at=FIXED_TIME,
    )
    registry.add(fixture.record)

    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
        MagicMock(),
        MagicMock(),
        clock,
    )

    watcher.poll_once()

    assert parent_connection.close_count == 0
    fixture.process.join.assert_not_called()
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.IN_PROGRESS


def test_execution_watcher_closes_resources_after_success_result_before_upload_and_done(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    events: list[str] = []
    result = SupervisedExecutionResult.succeeded(JOB_ID)
    parent_connection = _FakeParentConnection(
        result=result,
        close_callback=lambda: events.append("close_parent_connection"),
    )
    fixture = _record(
        tmp_path,
        parent_connection=parent_connection,
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    output_artifact_uploader = MagicMock()
    status_publisher = MagicMock()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    fixture.process.join.side_effect = lambda timeout: events.append(
        f"join_process:{timeout}"
    )
    output_artifact_uploader.upload.side_effect = lambda context: events.append(
        "upload_outputs"
    )
    status_publisher.publish_done.side_effect = lambda **_kwargs: events.append(
        "publish_done"
    )
    ack_mock.ack.side_effect = lambda: events.append("ack")

    watcher = _watcher(
        registry,
        coordinator,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    assert events == [
        "close_parent_connection",
        "join_process:0",
        "upload_outputs",
        "publish_done",
        "ack",
    ]

    assert parent_connection.close_count == 1
    fixture.process.join.assert_called_once_with(timeout=0)

    output_artifact_uploader.upload.assert_called_once_with(record.context)
    status_publisher.publish_done.assert_called_once()
    status_publisher.publish_error.assert_not_called()
    ack_mock.ack.assert_called_once()

    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.DONE


def test_execution_watcher_closes_resources_after_failed_result_before_error_publication(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    events: list[str] = []
    result = SupervisedExecutionResult(
        job_id=JOB_ID,
        status=SupervisedExecutionStatus.FAILED,
        message="execute failed",
        error_type="RuntimeError",
    )
    parent_connection = _FakeParentConnection(
        result=result,
        close_callback=lambda: events.append("close_parent_connection"),
    )
    fixture = _record(
        tmp_path,
        parent_connection=parent_connection,
        started_at=FIXED_TIME,
    )
    registry.add(fixture.record)

    status_publisher = MagicMock()
    ack_mock = MagicMock(spec=Acknowledger)
    ack_mock.ack.side_effect = lambda: events.append("ack")
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    fixture.process.join.side_effect = lambda timeout: events.append(
        f"join_process:{timeout}"
    )
    status_publisher.publish_error.side_effect = lambda **_kwargs: events.append(
        "publish_error"
    )

    watcher = _watcher(
        registry,
        coordinator,
        MagicMock(),
        status_publisher,
        clock,
    )

    watcher.poll_once()

    assert events == [
        "close_parent_connection",
        "join_process:0",
        "publish_error",
        "ack",
    ]

    assert parent_connection.close_count == 1
    fixture.process.join.assert_called_once_with(timeout=0)

    status_publisher.publish_error.assert_called_once_with(
        workspace=fixture.record.workspace,
        message="RuntimeError: execute failed",
    )
    status_publisher.publish_done.assert_not_called()
    ack_mock.ack.assert_called_once()

    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.ERROR


def test_execution_watcher_resource_close_failures_do_not_prevent_success_finalization(
    tmp_path: Path,
) -> None:
    clock = _MutableClock(FIXED_TIME + timedelta(seconds=10))
    registry = ExecutionRegistry()
    result = SupervisedExecutionResult.succeeded(JOB_ID)
    parent_connection = _FakeParentConnection(
        result=result,
        close_exception=RuntimeError("close failed"),
    )
    fixture = _record(
        tmp_path,
        parent_connection=parent_connection,
        started_at=FIXED_TIME,
    )
    record = fixture.record
    registry.add(record)

    fixture.process.join.side_effect = RuntimeError("join failed")

    output_artifact_uploader = MagicMock()
    status_publisher = MagicMock()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    watcher = _watcher(
        registry,
        coordinator,
        output_artifact_uploader,
        status_publisher,
        clock,
    )

    watcher.poll_once()

    assert parent_connection.close_count == 1
    fixture.process.join.assert_called_once_with(timeout=0)

    output_artifact_uploader.upload.assert_called_once_with(record.context)
    status_publisher.publish_done.assert_called_once()
    status_publisher.publish_error.assert_not_called()

    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.DONE


def _watcher(
    execution_registry: ExecutionRegistry,
    job_state_transition_coordinator: JobStateTransitionCoordinator,
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
        job_state_transition_coordinator=job_state_transition_coordinator,
        output_artifact_uploader=output_artifact_uploader,
        status_publisher=status_publisher,
        worker_id=WORKER_ID,
        job_timeout_seconds=job_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        progress_interval_seconds=progress_interval_seconds,
        clock=clock,
    )


def _coordinator_in_state(
    acknowledger: Acknowledger,
    state: WorkerJobStatus = WorkerJobStatus.IN_PROGRESS,
    job_id: str = JOB_ID,
) -> JobStateTransitionCoordinator:
    coordinator = JobStateTransitionCoordinator()
    record = coordinator.create(job_id=job_id, submitted_ack=acknowledger)
    with record.lock:
        record.state = state
    return coordinator


def _record(
    tmp_path: Path,
    *,
    job_id: str = JOB_ID,
    parent_connection: "_FakeParentConnection",
    started_at: datetime,
) -> "_RecordFixture":
    context = _context(tmp_path, job_id=job_id)
    process = MagicMock()

    process_record = ProcessRecord(
        process=process,
        parent_connection=parent_connection,
        started_at=started_at,
    )

    record = ExecutionRecord(
        workspace=context.workspace,
        context=context,
        process_record=process_record,
    )

    return _RecordFixture(
        record=record,
        process=process,
    )


def _context(tmp_path: Path, *, job_id: str = JOB_ID) -> JobExecutionContext:
    worker_id = WORKER_ID
    work_dir = tmp_path / "jobs" / "42" / job_id
    input_dir = work_dir / "in"
    output_dir = work_dir / "out"

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
        work_dir=work_dir,
        input_dir=input_dir,
        output_dir=output_dir,
        worker_id=worker_id,
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


@dataclass(frozen=True)
class _RecordFixture:
    record: ExecutionRecord
    process: MagicMock


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
        close_exception: BaseException | None = None,
        close_callback: Callable[[], None] | None = None,
    ) -> None:
        self._result = result
        self._poll_result = poll_result
        self._poll_exception = poll_exception
        self._recv_exception = recv_exception
        self._close_exception = close_exception
        self._close_callback = close_callback
        self.close_count = 0

    def poll(self) -> bool:
        if self._poll_exception is not None:
            raise self._poll_exception
        return self._poll_result

    def recv(self) -> Any:
        if self._recv_exception is not None:
            raise self._recv_exception
        return self._result

    def close(self) -> None:
        self.close_count += 1

        if self._close_callback is not None:
            self._close_callback()

        if self._close_exception is not None:
            raise self._close_exception

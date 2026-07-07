# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.domain.manifest import JobManifest
from mdds_worker_runtime.execution.models import (
    ExecutionRecord,
    WorkerJobStatus,
    ProcessRecord,
)
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.execution.timeout_watcher import TimeoutWatcher
from mdds_worker_runtime.execution.workspace import JobWorkspace
from mdds_worker_runtime.job_state import (
    JobStateTransitionCoordinator,
)
from mdds_worker_runtime.queue.queue_client import Acknowledger

FIXED_NOW = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
WORKER_ID = "worker-1"
JOB_ID = "job-1"
JOB_TIMEOUT_SECONDS = 10.0
JOIN_TIMEOUT_SECONDS = 0.01
TIMED_OUT_STARTED_AT = FIXED_NOW - timedelta(seconds=JOB_TIMEOUT_SECONDS)
NOT_TIMED_OUT_STARTED_AT = FIXED_NOW - timedelta(seconds=JOB_TIMEOUT_SECONDS - 1)


@pytest.mark.parametrize(
    ("field_name", "bad_value", "error_message"),
    [
        ("execution_registry", None, "execution_registry cannot be null."),
        (
            "job_state_transition_coordinator",
            None,
            "job_state_transition_coordinator cannot be null.",
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
            "job_timeout_seconds",
            -1,
            "job_timeout_seconds must be greater than zero.",
        ),
        (
            "poll_interval_seconds",
            0,
            "poll_interval_seconds must be greater than zero.",
        ),
        (
            "poll_interval_seconds",
            -1,
            "poll_interval_seconds must be greater than zero.",
        ),
        (
            "terminated_process_join_timeout_seconds",
            -1,
            "terminated_process_join_timeout_seconds must not be negative.",
        ),
        ("clock", None, "clock cannot be null."),
    ],
)
def test_timeout_watcher_rejects_invalid_constructor_arguments(
    field_name: str,
    bad_value: object,
    error_message: str,
) -> None:
    kwargs = {
        "execution_registry": ExecutionRegistry(),
        "job_state_transition_coordinator": JobStateTransitionCoordinator(),
        "status_publisher": MagicMock(),
        "worker_id": WORKER_ID,
        "job_timeout_seconds": JOB_TIMEOUT_SECONDS,
        "poll_interval_seconds": 0.01,
        "terminated_process_join_timeout_seconds": JOIN_TIMEOUT_SECONDS,
        "clock": lambda: FIXED_NOW,
        field_name: bad_value,
    }

    with pytest.raises(ValueError) as exc_info:
        TimeoutWatcher(**kwargs)

    assert str(exc_info.value) == error_message


def test_timeout_watcher_ignores_not_timed_out_record() -> None:
    registry = ExecutionRegistry()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)
    status_publisher = MagicMock()
    process = _FakeProcess(alive=True)
    record = _record(
        registry,
        started_at=NOT_TIMED_OUT_STARTED_AT,
        process=process,
    )

    watcher = _watcher(registry, coordinator, status_publisher)

    watcher.poll_once()

    assert registry.get(record.job_id) is record
    assert coordinator.get_state(record.job_id) is WorkerJobStatus.IN_PROGRESS
    assert process.terminate_count == 0
    assert process.join_count == 0
    status_publisher.publish_error.assert_not_called()
    ack_mock.ack.assert_not_called()


def test_timeout_watcher_ignores_already_terminal_worker_state() -> None:
    registry = ExecutionRegistry()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack, WorkerJobStatus.DONE)
    status_publisher = MagicMock()
    process = _FakeProcess(alive=True)
    record = _record(
        registry,
        started_at=TIMED_OUT_STARTED_AT,
        process=process,
    )

    watcher = _watcher(registry, coordinator, status_publisher)

    watcher.poll_once()

    assert registry.get(record.job_id) is record
    assert coordinator.get_state(record.job_id) is WorkerJobStatus.DONE
    assert process.terminate_count == 0
    assert process.join_count == 0
    status_publisher.publish_error.assert_not_called()
    ack_mock.ack.assert_not_called()


def test_timeout_watcher_ignores_timed_out_record_when_process_is_not_alive() -> None:
    registry = ExecutionRegistry()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)
    status_publisher = MagicMock()
    process = _FakeProcess(alive=False)
    record = _record(
        registry,
        started_at=TIMED_OUT_STARTED_AT,
        process=process,
    )

    watcher = _watcher(registry, coordinator, status_publisher)

    watcher.poll_once()

    assert registry.get(record.job_id) is record
    assert coordinator.get_state(record.job_id) is WorkerJobStatus.IN_PROGRESS
    assert process.terminate_count == 0
    assert process.join_count == 0
    status_publisher.publish_error.assert_not_called()
    ack_mock.ack.assert_not_called()


def test_timeout_watcher_finalizes_timed_out_alive_process_as_error() -> None:
    registry = ExecutionRegistry()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)
    status_publisher = MagicMock()
    process = _FakeProcess(alive=True, alive_after_join=False)
    parent_connection = MagicMock()
    record = _record(
        registry,
        started_at=TIMED_OUT_STARTED_AT,
        process=process,
        parent_connection=parent_connection,
    )

    watcher = _watcher(registry, coordinator, status_publisher)

    watcher.poll_once()

    assert registry.get(record.job_id) is record
    assert coordinator.get_state(record.job_id) is WorkerJobStatus.ERROR

    assert process.terminate_count == 1
    assert process.join_count == 1
    assert process.join_timeout == JOIN_TIMEOUT_SECONDS
    parent_connection.close.assert_called_once()
    status_publisher.publish_error.assert_called_once_with(
        workspace=record.workspace,
        message=(
            "Job execution exceeded runtime timeout and was terminated: "
            f"jobId='{JOB_ID}', timeoutSeconds={JOB_TIMEOUT_SECONDS:g}."
        ),
    )
    ack_mock.ack.assert_called_once()


def test_timeout_watcher_publish_error_failure_does_not_ack_or_commit_error() -> None:
    registry = ExecutionRegistry()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)
    status_publisher = MagicMock()
    status_publisher.publish_error.side_effect = RuntimeError("publish failed")

    process = _FakeProcess(alive=True, alive_after_join=False)
    parent_connection = MagicMock()
    record = _record(
        registry,
        started_at=TIMED_OUT_STARTED_AT,
        process=process,
        parent_connection=parent_connection,
    )

    watcher = _watcher(registry, coordinator, status_publisher)

    watcher.poll_once()

    assert registry.get(record.job_id) is record
    assert coordinator.get_state(record.job_id) is WorkerJobStatus.IN_PROGRESS

    assert process.terminate_count == 1
    assert process.join_count == 1
    parent_connection.close.assert_called_once()
    status_publisher.publish_error.assert_called_once()
    ack_mock.ack.assert_not_called()


def test_timeout_watcher_ack_failure_does_not_propagate_after_error_commit() -> None:
    registry = ExecutionRegistry()
    ack_mock = MagicMock(spec=Acknowledger)
    ack_mock.ack.side_effect = RuntimeError("ack failed")
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)

    status_publisher = MagicMock()
    process = _FakeProcess(alive=True, alive_after_join=False)
    parent_connection = MagicMock()
    record = _record(
        registry,
        started_at=TIMED_OUT_STARTED_AT,
        process=process,
        parent_connection=parent_connection,
    )

    watcher = _watcher(registry, coordinator, status_publisher)

    watcher.poll_once()

    assert registry.get(record.job_id) is record
    assert coordinator.get_state(record.job_id) is WorkerJobStatus.ERROR

    assert process.terminate_count == 1
    assert process.join_count == 1
    parent_connection.close.assert_called_once()

    status_publisher.publish_error.assert_called_once_with(
        workspace=record.workspace,
        message=(
            "Job execution exceeded runtime timeout and was terminated: "
            f"jobId='{JOB_ID}', timeoutSeconds={JOB_TIMEOUT_SECONDS:g}."
        ),
    )
    ack_mock.ack.assert_called_once()


def test_timeout_watcher_terminate_failure_does_not_publish_error_or_ack() -> None:
    registry = ExecutionRegistry()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)
    status_publisher = MagicMock()
    process = _FakeProcess(alive=True, alive_after_join=True)
    parent_connection = MagicMock()
    record = _record(
        registry,
        started_at=TIMED_OUT_STARTED_AT,
        process=process,
        parent_connection=parent_connection,
    )

    watcher = _watcher(registry, coordinator, status_publisher)

    watcher.poll_once()

    assert registry.get(record.job_id) is record
    assert coordinator.get_state(record.job_id) is WorkerJobStatus.IN_PROGRESS

    assert process.terminate_count == 1
    assert process.join_count == 1
    parent_connection.close.assert_not_called()
    status_publisher.publish_error.assert_not_called()
    ack_mock.ack.assert_not_called()


def test_timeout_watcher_parent_connection_close_failure_still_finalizes_error() -> (
    None
):
    registry = ExecutionRegistry()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)
    status_publisher = MagicMock()
    process = _FakeProcess(alive=True, alive_after_join=False)
    parent_connection = MagicMock()
    parent_connection.close.side_effect = RuntimeError("close failed")
    record = _record(
        registry,
        started_at=TIMED_OUT_STARTED_AT,
        process=process,
        parent_connection=parent_connection,
    )

    watcher = _watcher(registry, coordinator, status_publisher)

    watcher.poll_once()

    assert registry.get(record.job_id) is record
    assert coordinator.get_state(record.job_id) is WorkerJobStatus.ERROR

    assert process.terminate_count == 1
    assert process.join_count == 1
    parent_connection.close.assert_called_once()
    status_publisher.publish_error.assert_called_once()
    ack_mock.ack.assert_called_once()


def test_timeout_watcher_start_stop_are_idempotent() -> None:
    watcher = _watcher(
        ExecutionRegistry(),
        JobStateTransitionCoordinator(),
        MagicMock(),
        poll_interval_seconds=0.01,
    )

    watcher.start()
    watcher.start()
    watcher.stop(timeout_seconds=1.0)
    watcher.stop(timeout_seconds=1.0)


def test_timeout_watcher_run_loop_logs_polling_failure_and_stops() -> None:
    watcher = _watcher(
        ExecutionRegistry(),
        JobStateTransitionCoordinator(),
        MagicMock(),
        poll_interval_seconds=0.01,
    )

    def fail_and_stop() -> None:
        watcher._stop_requested.set()  # pylint: disable=protected-access
        raise RuntimeError("poll failed")

    poll_once_mock = MagicMock(side_effect=fail_and_stop)
    setattr(watcher, "poll_once", poll_once_mock)

    watcher._run_loop()  # pylint: disable=protected-access

    poll_once_mock.assert_called_once()


def test_timeout_watcher_ignores_none_record() -> None:
    registry = MagicMock()
    registry.snapshot.return_value = [None]
    coordinator = JobStateTransitionCoordinator()
    status_publisher = MagicMock()

    watcher = _watcher(registry, coordinator, status_publisher)

    watcher.poll_once()

    status_publisher.publish_error.assert_not_called()


def test_timeout_watcher_process_liveness_check_failure_does_not_finalize() -> None:
    registry = ExecutionRegistry()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)
    status_publisher = MagicMock()
    process = _FakeProcess(
        alive=True,
        is_alive_exception=RuntimeError("is_alive failed"),
    )
    record = _record(
        registry,
        started_at=TIMED_OUT_STARTED_AT,
        process=process,
    )

    watcher = _watcher(registry, coordinator, status_publisher)

    watcher.poll_once()

    assert registry.get(record.job_id) is record
    assert coordinator.get_state(record.job_id) is WorkerJobStatus.IN_PROGRESS

    assert process.terminate_count == 0
    assert process.join_count == 0
    status_publisher.publish_error.assert_not_called()
    ack_mock.ack.assert_not_called()


def test_timeout_watcher_joins_but_does_not_terminate_when_process_stops_before_termination() -> (
    None
):
    registry = ExecutionRegistry()
    ack_mock = MagicMock(spec=Acknowledger)
    ack = cast(Acknowledger, cast(object, ack_mock))
    coordinator = _coordinator_in_state(ack)
    status_publisher = MagicMock()
    process = _FakeProcess(
        alive=True,
        is_alive_results=[True, False, False],
    )
    parent_connection = MagicMock()
    record = _record(
        registry,
        started_at=TIMED_OUT_STARTED_AT,
        process=process,
        parent_connection=parent_connection,
    )

    watcher = _watcher(registry, coordinator, status_publisher)

    watcher.poll_once()

    assert registry.get(record.job_id) is record
    assert coordinator.get_state(record.job_id) is WorkerJobStatus.ERROR

    assert process.terminate_count == 0
    assert process.join_count == 1
    assert process.join_timeout == JOIN_TIMEOUT_SECONDS
    parent_connection.close.assert_called_once()
    status_publisher.publish_error.assert_called_once()
    ack_mock.ack.assert_called_once()


def _watcher(
    execution_registry: ExecutionRegistry,
    job_state_transition_coordinator: JobStateTransitionCoordinator,
    status_publisher: MagicMock,
    *,
    poll_interval_seconds: float = 0.01,
) -> TimeoutWatcher:
    return TimeoutWatcher(
        execution_registry=execution_registry,
        job_state_transition_coordinator=job_state_transition_coordinator,
        status_publisher=status_publisher,
        worker_id=WORKER_ID,
        job_timeout_seconds=JOB_TIMEOUT_SECONDS,
        poll_interval_seconds=poll_interval_seconds,
        terminated_process_join_timeout_seconds=JOIN_TIMEOUT_SECONDS,
        clock=lambda: FIXED_NOW,
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
    registry: ExecutionRegistry,
    *,
    job_id: str = JOB_ID,
    started_at: datetime,
    process: "_FakeProcess | None" = None,
    parent_connection: MagicMock | None = None,
) -> ExecutionRecord:
    manifest = JobManifest(
        manifest_version=1,
        user_id=42,
        job_id=job_id,
        job_type="SOLVING_SLAE",
        inputs={},
        params={},
        outputs={},
    )

    work_dir = Path("/tmp/jobs") / str(manifest.user_id) / manifest.job_id

    workspace = JobWorkspace(
        manifest=manifest,
        work_dir=work_dir,
        input_dir=work_dir / "in",
        output_dir=work_dir / "out",
        worker_id=WORKER_ID,
    )

    context = MagicMock()
    context.workspace = workspace
    context.user_id = workspace.user_id
    context.job_id = workspace.job_id
    context.job_type = workspace.job_type
    context.work_dir = workspace.work_dir
    context.input_dir = workspace.input_dir
    context.output_dir = workspace.output_dir

    record = ExecutionRecord(
        workspace=workspace,
        context=context,
        process_record=ProcessRecord(
            process=process or _FakeProcess(alive=True),
            parent_connection=parent_connection or MagicMock(),
            started_at=started_at,
        ),
    )

    registry.add(record)
    return record


class _FakeProcess:
    def __init__(
        self,
        *,
        alive: bool,
        alive_after_join: bool = False,
        is_alive_results: list[bool] | None = None,
        is_alive_exception: Exception | None = None,
        terminate_exception: Exception | None = None,
        join_exception: Exception | None = None,
    ) -> None:
        self._alive = alive
        self._alive_after_join = alive_after_join
        self._is_alive_results = list(is_alive_results or [])
        self._is_alive_exception = is_alive_exception
        self._terminate_exception = terminate_exception
        self._join_exception = join_exception

        self.is_alive_count = 0
        self.terminate_count = 0
        self.join_count = 0
        self.join_timeout: float | None = None

    def is_alive(self) -> bool:
        self.is_alive_count += 1

        if self._is_alive_exception is not None:
            raise self._is_alive_exception

        if self._is_alive_results:
            return self._is_alive_results.pop(0)

        return self._alive

    def terminate(self) -> None:
        self.terminate_count += 1
        if self._terminate_exception is not None:
            raise self._terminate_exception

    def join(self, timeout: float | None = None) -> None:
        self.join_count += 1
        self.join_timeout = timeout
        if self._join_exception is not None:
            raise self._join_exception

        self._alive = self._alive_after_join

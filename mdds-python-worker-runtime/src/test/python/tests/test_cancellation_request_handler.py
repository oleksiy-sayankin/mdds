# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.domain.manifest import JobManifest
from mdds_worker_runtime.execution.cancellation import CancellationRequestHandler
from mdds_worker_runtime.execution.models import (
    ExecutionRecord,
    WorkerJobStatus,
    ProcessRecord,
)
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.execution.status_publisher import StatusPublisher
from mdds_worker_runtime.execution.workspace import JobWorkspace
from mdds_worker_runtime.job_state import (
    JobStateTransitionCoordinator,
)
from mdds_worker_runtime.queue.queue_client import Acknowledger

WORKER_ID = "worker-1"
JOB_ID = "job-1"
USER_ID = 42
JOB_TYPE = "SOLVING_SLAE"
FIXED_NOW = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
JOIN_TIMEOUT_SECONDS = 0.1


def test_constructor_rejects_null_execution_registry() -> None:
    with pytest.raises(ValueError, match="execution_registry cannot be null."):
        CancellationRequestHandler(
            execution_registry=cast(ExecutionRegistry, cast(object, None)),
            status_publisher=cast(StatusPublisher, cast(object, _StatusPublisherSpy())),
            worker_id=WORKER_ID,
            clock=_clock,
        )


def test_constructor_rejects_null_status_publisher() -> None:
    with pytest.raises(ValueError, match="status_publisher cannot be null."):
        CancellationRequestHandler(
            execution_registry=ExecutionRegistry(),
            status_publisher=cast(StatusPublisher, cast(object, None)),
            worker_id=WORKER_ID,
            clock=_clock,
        )


@pytest.mark.parametrize("worker_id", [None, "", " "])
def test_constructor_rejects_blank_worker_id(worker_id: str | None) -> None:
    with pytest.raises(ValueError, match="worker_id cannot be null or blank."):
        CancellationRequestHandler(
            execution_registry=ExecutionRegistry(),
            status_publisher=cast(StatusPublisher, cast(object, _StatusPublisherSpy())),
            worker_id=cast(str, worker_id),
            clock=_clock,
        )


def test_constructor_rejects_negative_cancelled_process_join_timeout_seconds() -> None:
    with pytest.raises(
        ValueError,
        match="cancelled_process_join_timeout_seconds must not be negative.",
    ):
        CancellationRequestHandler(
            execution_registry=ExecutionRegistry(),
            status_publisher=cast(StatusPublisher, cast(object, _StatusPublisherSpy())),
            worker_id=WORKER_ID,
            cancelled_process_join_timeout_seconds=-0.1,
            clock=_clock,
        )


def test_constructor_rejects_null_clock() -> None:
    with pytest.raises(ValueError, match="clock cannot be null."):
        CancellationRequestHandler(
            execution_registry=ExecutionRegistry(),
            status_publisher=cast(StatusPublisher, cast(object, _StatusPublisherSpy())),
            worker_id=WORKER_ID,
            clock=None,
        )


@pytest.mark.parametrize("job_id", [None, "", " "])
def test_accept_cancellation_request_rejects_blank_job_id(
    job_id: str | None,
) -> None:
    handler = _handler()

    with pytest.raises(ValueError, match="job_id cannot be null or blank"):
        handler.finalize_cancellation(cast(str, job_id))


@pytest.mark.parametrize("job_id", [None, "", " "])
def test_finalize_cancellation_if_current_rejects_blank_job_id(
    job_id: str | None,
) -> None:
    handler = _handler()

    with pytest.raises(ValueError, match="job_id cannot be null or blank"):
        handler.finalize_cancellation(cast(str, job_id))


def test_finalize_cancellation_ignores_missing_record() -> None:
    registry = ExecutionRegistry()
    coordinator = _coordinator_in_state(WorkerJobStatus.IN_PROGRESS)
    status_publisher = _StatusPublisherSpy()
    handler = _handler(
        execution_registry=registry,
        status_publisher=status_publisher,
    )

    with pytest.raises(
        RuntimeError, match="Cancellation request cannot be accepted by Worker."
    ):
        handler.finalize_cancellation(JOB_ID)

    assert status_publisher.cancelled == []
    assert coordinator.get_state(JOB_ID) is WorkerJobStatus.IN_PROGRESS


def test_finalize_cancellation_if_current_terminates_alive_process(
    tmp_path: Path,
) -> None:
    process = _ProcessFake(alive=True, terminate_stops=True)
    fixture = _registered_fixture(tmp_path, process=process)

    fixture.handler.finalize_cancellation(JOB_ID)

    assert process.terminate_count == 1
    assert process.kill_count == 0
    assert process.join_timeouts == [JOIN_TIMEOUT_SECONDS]


def test_finalize_cancellation_if_current_joins_process_after_terminate(
    tmp_path: Path,
) -> None:
    process = _ProcessFake(alive=True, terminate_stops=True)
    fixture = _registered_fixture(tmp_path, process=process)

    fixture.handler.finalize_cancellation(JOB_ID)

    assert process.events == [
        "terminate",
        f"join:{JOIN_TIMEOUT_SECONDS}",
    ]


def test_finalize_cancellation_publishes_cancelled(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    fixture = _registered_fixture(
        tmp_path,
        status_publisher=_StatusPublisherSpy(events=events),
        process=_ProcessFake(events=events),
        parent_connection=_ParentConnectionFake(events=events),
    )

    fixture.handler.finalize_cancellation(JOB_ID)

    assert "publish_cancelled" in events
    assert len(fixture.status_publisher.cancelled) == 1
    assert fixture.status_publisher.cancelled[0]["job_id"] == JOB_ID
    assert fixture.status_publisher.cancelled[0]["worker_id"] == WORKER_ID


def test_finalize_cancellation_if_current_does_not_publish_done(tmp_path: Path) -> None:
    fixture = _registered_fixture(tmp_path)

    fixture.handler.finalize_cancellation(JOB_ID)

    assert fixture.status_publisher.done == []


def test_finalize_cancellation_if_current_does_not_publish_error(
    tmp_path: Path,
) -> None:
    fixture = _registered_fixture(tmp_path)

    fixture.handler.finalize_cancellation(JOB_ID)

    assert fixture.status_publisher.error == []


def test_finalize_cancellation_if_current_does_not_remove_record_from_registry(
    tmp_path: Path,
) -> None:
    fixture = _registered_fixture(tmp_path)

    fixture.handler.finalize_cancellation(JOB_ID)

    assert fixture.registry.size() == 1
    assert fixture.registry.get(JOB_ID) is fixture.record


def test_finalize_cancellation_if_current_does_not_delete_local_workspace(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "jobs" / str(USER_ID) / JOB_ID
    workspace.mkdir(parents=True)
    fixture = _registered_fixture(tmp_path, workspace=workspace)

    fixture.handler.finalize_cancellation(JOB_ID)

    assert workspace.exists()
    assert workspace.is_dir()


def _handler(
    *,
    execution_registry: ExecutionRegistry | None = None,
    status_publisher: _StatusPublisherSpy | None = None,
) -> CancellationRequestHandler:
    return CancellationRequestHandler(
        execution_registry=execution_registry or ExecutionRegistry(),
        status_publisher=cast(
            StatusPublisher,
            cast(object, status_publisher or _StatusPublisherSpy()),
        ),
        worker_id=WORKER_ID,
        cancelled_process_join_timeout_seconds=JOIN_TIMEOUT_SECONDS,
        clock=_clock,
    )


def _registered_fixture(
    tmp_path: Path,
    *,
    process: Any | None = None,
    parent_connection: "_ParentConnectionFake | None" = None,
    status_publisher: _StatusPublisherSpy | None = None,
    submitted_ack: "_SubmittedAckSpy | None" = None,
    workspace: Path | None = None,
) -> "_RegisteredFixture":
    registry = ExecutionRegistry()
    coordinator = _coordinator_in_state(WorkerJobStatus.IN_PROGRESS)
    resolved_status_publisher = status_publisher or _StatusPublisherSpy()
    resolved_submitted_ack = submitted_ack or _SubmittedAckSpy()
    resolved_process = process or _ProcessFake()
    resolved_parent_connection = parent_connection or _ParentConnectionFake()

    record = _record(
        tmp_path,
        process=resolved_process,
        parent_connection=resolved_parent_connection,
        submitted_ack=resolved_submitted_ack,
        workspace=workspace,
    )
    registry.add(record)

    handler = _handler(
        execution_registry=registry,
        status_publisher=resolved_status_publisher,
    )

    return _RegisteredFixture(
        registry=registry,
        coordinator=coordinator,
        status_publisher=resolved_status_publisher,
        submitted_ack=resolved_submitted_ack,
        process=resolved_process,
        parent_connection=resolved_parent_connection,
        record=record,
        handler=handler,
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
    process: Any | None = None,
    parent_connection: "_ParentConnectionFake | None" = None,
    submitted_ack: "_SubmittedAckSpy | None" = None,
    workspace: Path | None = None,
) -> ExecutionRecord:
    del submitted_ack

    work_dir = workspace or (tmp_path / "jobs" / str(USER_ID) / job_id)

    manifest = JobManifest(
        manifest_version=1,
        user_id=USER_ID,
        job_id=job_id,
        job_type=JOB_TYPE,
        inputs={},
        params={},
        outputs={},
    )

    job_workspace = JobWorkspace(
        manifest=manifest,
        work_dir=work_dir,
        input_dir=work_dir / "in",
        output_dir=work_dir / "out",
        worker_id=WORKER_ID,
    )

    context = MagicMock()
    context.workspace = job_workspace
    context.work_dir = job_workspace.work_dir
    context.input_dir = job_workspace.input_dir
    context.output_dir = job_workspace.output_dir
    context.user_id = job_workspace.user_id
    context.job_id = job_workspace.job_id
    context.job_type = job_workspace.job_type

    return ExecutionRecord(
        workspace=job_workspace,
        context=context,
        process_record=ProcessRecord(
            process=process or _ProcessFake(),
            parent_connection=parent_connection or _ParentConnectionFake(),
            started_at=FIXED_NOW,
        ),
    )


def _clock() -> datetime:
    return FIXED_NOW


@dataclass(frozen=True)
class _RegisteredFixture:
    registry: ExecutionRegistry
    coordinator: JobStateTransitionCoordinator
    status_publisher: "_StatusPublisherSpy"
    submitted_ack: "_SubmittedAckSpy"
    process: Any
    parent_connection: "_ParentConnectionFake"
    record: ExecutionRecord
    handler: CancellationRequestHandler


class _StatusPublisherSpy:
    def __init__(
        self,
        *,
        events: list[str] | None = None,
        publish_cancelled_error: Exception | None = None,
    ) -> None:
        self._events = events
        self._publish_cancelled_error = publish_cancelled_error

        self.cancelled: list[dict[str, Any]] = []
        self.done: list[dict[str, Any]] = []
        self.error: list[dict[str, Any]] = []

    def publish_cancelled(
        self,
        workspace: JobWorkspace,
        message: str,
    ) -> None:
        if self._events is not None:
            self._events.append("publish_cancelled")

        if self._publish_cancelled_error is not None:
            raise self._publish_cancelled_error

        self.cancelled.append(
            {
                "user_id": workspace.user_id,
                "job_id": workspace.job_id,
                "job_type": workspace.job_type,
                "worker_id": workspace.worker_id,
                "message": message,
            }
        )

    def publish_done(
        self,
        workspace: JobWorkspace,
        message: str = "",
    ) -> None:
        self.done.append(
            {
                "user_id": workspace.user_id,
                "job_id": workspace.job_id,
                "job_type": workspace.job_type,
                "worker_id": workspace.worker_id,
                "message": message,
            }
        )

    def publish_error(
        self,
        workspace: JobWorkspace,
        message: str,
    ) -> None:
        self.error.append(
            {
                "user_id": workspace.user_id,
                "job_id": workspace.job_id,
                "job_type": workspace.job_type,
                "worker_id": workspace.worker_id,
                "message": message,
            }
        )


class _SubmittedAckSpy:
    def __init__(
        self,
        *,
        events: list[str] | None = None,
        ack_error: Exception | None = None,
    ) -> None:
        self._events = events
        self._ack_error = ack_error
        self.ack_count = 0

    def ack(self) -> None:
        self.ack_count += 1

        if self._events is not None:
            self._events.append("ack")

        if self._ack_error is not None:
            raise self._ack_error

    def nack(self, requeue: bool) -> None:
        if self._events is not None:
            self._events.append(f"nack:{requeue}")


class _ParentConnectionFake:
    def __init__(
        self,
        *,
        events: list[str] | None = None,
        close_error: Exception | None = None,
    ) -> None:
        self._events = events
        self._close_error = close_error
        self.close_count = 0

    def close(self) -> None:
        self.close_count += 1

        if self._events is not None:
            self._events.append("close_parent_connection")

        if self._close_error is not None:
            raise self._close_error


class _ProcessFake:
    def __init__(
        self,
        *,
        alive: bool = True,
        terminate_stops: bool = True,
        kill_stops: bool = True,
        events: list[str] | None = None,
    ) -> None:
        self._alive = alive
        self._terminate_stops = terminate_stops
        self._kill_stops = kill_stops
        self.events: list[str] = []
        self._external_events = events

        self.terminate_count = 0
        self.kill_count = 0
        self.join_timeouts: list[float | None] = []

    def is_alive(self) -> bool:
        return self._alive

    def terminate(self) -> None:
        self.terminate_count += 1
        self._append_event("terminate")

        if self._terminate_stops:
            self._alive = False

    def kill(self) -> None:
        self.kill_count += 1
        self._append_event("kill")

        if self._kill_stops:
            self._alive = False

    def join(self, timeout: float | None = None) -> None:
        self.join_timeouts.append(timeout)
        self._append_event(f"join:{timeout}")

    def _append_event(self, event: str) -> None:
        self.events.append(event)
        if self._external_events is not None:
            self._external_events.append(event)


class _ProcessWithoutKillFake:
    def __init__(self, *, alive: bool = True) -> None:
        self._alive = alive
        self.terminate_count = 0
        self.join_timeouts: list[float | None] = []

    def is_alive(self) -> bool:
        return self._alive

    def terminate(self) -> None:
        self.terminate_count += 1

    def join(self, timeout: float | None = None) -> None:
        self.join_timeouts.append(timeout)

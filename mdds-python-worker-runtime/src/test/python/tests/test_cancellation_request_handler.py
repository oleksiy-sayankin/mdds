# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.execution.cancellation import CancellationRequestHandler
from mdds_worker_runtime.execution.models import ExecutionRecord, WorkerJobStatus
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.execution.status_publisher import StatusPublisher

WORKER_ID = "worker-1"
JOB_ID = "job-1"
USER_ID = 42
JOB_TYPE = "SOLVING_SLAE"
MANIFEST_OBJECT_KEY = f"jobs/{USER_ID}/{JOB_ID}/manifest.json"
FIXED_NOW = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
JOIN_TIMEOUT_SECONDS = 0.1


def test_constructor_rejects_null_execution_registry() -> None:
    with pytest.raises(ValueError, match="execution_registry cannot be null."):
        CancellationRequestHandler(
            execution_registry=cast(ExecutionRegistry, None),
            status_publisher=cast(StatusPublisher, _StatusPublisherSpy()),
            worker_id=WORKER_ID,
            clock=_clock,
        )


def test_constructor_rejects_null_status_publisher() -> None:
    with pytest.raises(ValueError, match="status_publisher cannot be null."):
        CancellationRequestHandler(
            execution_registry=ExecutionRegistry(),
            status_publisher=cast(StatusPublisher, None),
            worker_id=WORKER_ID,
            clock=_clock,
        )


@pytest.mark.parametrize("worker_id", [None, "", " "])
def test_constructor_rejects_blank_worker_id(worker_id: str | None) -> None:
    with pytest.raises(ValueError, match="worker_id cannot be null or blank."):
        CancellationRequestHandler(
            execution_registry=ExecutionRegistry(),
            status_publisher=cast(StatusPublisher, _StatusPublisherSpy()),
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
            status_publisher=cast(StatusPublisher, _StatusPublisherSpy()),
            worker_id=WORKER_ID,
            cancelled_process_join_timeout_seconds=-0.1,
            clock=_clock,
        )


def test_constructor_rejects_null_clock() -> None:
    with pytest.raises(ValueError, match="clock cannot be null."):
        CancellationRequestHandler(
            execution_registry=ExecutionRegistry(),
            status_publisher=cast(StatusPublisher, _StatusPublisherSpy()),
            worker_id=WORKER_ID,
            clock=None,
        )


@pytest.mark.parametrize("job_id", [None, "", " "])
def test_request_cancellation_rejects_blank_job_id(job_id: str | None) -> None:
    handler = _handler()

    with pytest.raises(ValueError, match="job_id cannot be null or blank."):
        handler.request_cancellation(cast(str, job_id))


def test_request_cancellation_ignores_missing_local_execution_record() -> None:
    registry = ExecutionRegistry()
    status_publisher = _StatusPublisherSpy()
    handler = _handler(
        execution_registry=registry,
        status_publisher=status_publisher,
    )

    handler.request_cancellation(JOB_ID)

    assert status_publisher.cancelled == []
    assert status_publisher.done == []
    assert status_publisher.error == []
    assert registry.size() == 0


def test_request_cancellation_ignores_already_terminal_claimed_record(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    status_publisher = _StatusPublisherSpy()
    process = _ProcessFake(alive=True)
    submitted_ack = _SubmittedAckSpy()
    record = _record(
        tmp_path,
        process=process,
        submitted_ack=submitted_ack,
    )
    registry.add(record)
    registry.try_claim_terminal(
        job_id=JOB_ID,
        terminal_status=WorkerJobStatus.DONE,
        message="Already done.",
        finished_at=FIXED_NOW,
    )

    handler = _handler(
        execution_registry=registry,
        status_publisher=status_publisher,
    )

    handler.request_cancellation(JOB_ID)

    assert process.terminate_count == 0
    assert process.kill_count == 0
    assert process.join_timeouts == []
    assert submitted_ack.ack_count == 0
    assert status_publisher.cancelled == []
    assert record.terminal_status == WorkerJobStatus.DONE
    assert record.terminal_status_published is False
    assert record.acknowledgement_done is False
    assert record.cleanup_ready is False
    assert registry.get(JOB_ID) is record


def test_request_cancellation_does_not_finalize_when_terminal_claim_fails(
    tmp_path: Path,
) -> None:
    registry = MagicMock(spec=ExecutionRegistry)
    status_publisher = _StatusPublisherSpy()
    process = _ProcessFake(alive=True)
    submitted_ack = _SubmittedAckSpy()
    record = _record(
        tmp_path,
        process=process,
        submitted_ack=submitted_ack,
    )

    registry.get.return_value = record
    registry.try_claim_terminal.return_value = None

    handler = _handler(
        execution_registry=cast(ExecutionRegistry, registry),
        status_publisher=status_publisher,
    )

    handler.request_cancellation(JOB_ID)

    registry.try_claim_terminal.assert_called_once()
    registry.mark_terminal_published.assert_not_called()
    registry.mark_acknowledged.assert_not_called()
    registry.mark_cleanup_ready.assert_not_called()

    assert process.terminate_count == 0
    assert process.kill_count == 0
    assert process.join_timeouts == []
    assert submitted_ack.ack_count == 0
    assert status_publisher.cancelled == []


def test_request_cancellation_finalizes_running_job_as_cancelled(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    status_publisher = _StatusPublisherSpy()
    submitted_ack = _SubmittedAckSpy()
    parent_connection = _ParentConnectionFake()
    process = _ProcessFake(alive=True, terminate_stops=True)
    record = _record(
        tmp_path,
        process=process,
        parent_connection=parent_connection,
        submitted_ack=submitted_ack,
    )
    registry.add(record)

    handler = _handler(
        execution_registry=registry,
        status_publisher=status_publisher,
    )

    handler.request_cancellation(JOB_ID)

    assert process.terminate_count == 1
    assert process.kill_count == 0
    assert process.join_timeouts == [JOIN_TIMEOUT_SECONDS]
    assert parent_connection.close_count == 1

    assert len(status_publisher.cancelled) == 1
    published = status_publisher.cancelled[0]
    assert published["user_id"] == USER_ID
    assert published["job_id"] == JOB_ID
    assert published["job_type"] == JOB_TYPE
    assert published["worker_id"] == WORKER_ID
    assert "Job cancellation requested and applied" in published["message"]

    assert submitted_ack.ack_count == 1

    assert record.terminal_status_claimed is True
    assert record.terminal_status == WorkerJobStatus.CANCELLED
    assert record.terminal_message is not None
    assert record.finished_at == FIXED_NOW
    assert record.terminal_status_published is True
    assert record.acknowledgement_done is True
    assert record.cleanup_ready is True
    assert registry.get(JOB_ID) is record


def test_request_cancellation_trims_job_id_before_lookup_and_finalization(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    status_publisher = _StatusPublisherSpy()
    record = _record(tmp_path)
    registry.add(record)

    handler = _handler(
        execution_registry=registry,
        status_publisher=status_publisher,
    )

    handler.request_cancellation(f"  {JOB_ID}  ")

    assert len(status_publisher.cancelled) == 1
    assert status_publisher.cancelled[0]["job_id"] == JOB_ID
    assert record.terminal_status == WorkerJobStatus.CANCELLED
    assert record.cleanup_ready is True


def test_request_cancellation_terminates_alive_process(tmp_path: Path) -> None:
    process = _ProcessFake(alive=True, terminate_stops=True)
    fixture = _registered_fixture(tmp_path, process=process)

    fixture.handler.request_cancellation(JOB_ID)

    assert process.terminate_count == 1
    assert process.kill_count == 0
    assert process.join_timeouts == [JOIN_TIMEOUT_SECONDS]


def test_request_cancellation_joins_process_after_terminate(tmp_path: Path) -> None:
    process = _ProcessFake(alive=True, terminate_stops=True)
    fixture = _registered_fixture(tmp_path, process=process)

    fixture.handler.request_cancellation(JOB_ID)

    assert process.events == [
        "terminate",
        f"join:{JOIN_TIMEOUT_SECONDS}",
    ]


def test_request_cancellation_does_not_terminate_stopped_process_but_still_joins(
    tmp_path: Path,
) -> None:
    process = _ProcessFake(alive=False)
    fixture = _registered_fixture(tmp_path, process=process)

    fixture.handler.request_cancellation(JOB_ID)

    assert process.terminate_count == 0
    assert process.kill_count == 0
    assert process.join_timeouts == [JOIN_TIMEOUT_SECONDS]


def test_request_cancellation_kills_process_when_terminate_and_join_do_not_stop_it(
    tmp_path: Path,
) -> None:
    process = _ProcessFake(
        alive=True,
        terminate_stops=False,
        kill_stops=True,
    )
    fixture = _registered_fixture(tmp_path, process=process)

    fixture.handler.request_cancellation(JOB_ID)

    assert process.terminate_count == 1
    assert process.kill_count == 1
    assert process.join_timeouts == [
        JOIN_TIMEOUT_SECONDS,
        JOIN_TIMEOUT_SECONDS,
    ]


def test_request_cancellation_fails_when_process_remains_alive_after_kill(
    tmp_path: Path,
) -> None:
    process = _ProcessFake(
        alive=True,
        terminate_stops=False,
        kill_stops=False,
    )
    fixture = _registered_fixture(tmp_path, process=process)

    with pytest.raises(
        RuntimeError, match="Cancelled supervised process is still alive"
    ):
        fixture.handler.request_cancellation(JOB_ID)

    assert process.terminate_count == 1
    assert process.kill_count == 1
    assert fixture.status_publisher.cancelled == []
    assert fixture.submitted_ack.ack_count == 0
    assert fixture.record.terminal_status_claimed is True
    assert fixture.record.terminal_status == WorkerJobStatus.CANCELLED
    assert fixture.record.terminal_status_published is False
    assert fixture.record.acknowledgement_done is False
    assert fixture.record.cleanup_ready is False
    assert fixture.registry.get(JOB_ID) is fixture.record


def test_request_cancellation_does_not_publish_cancelled_when_process_stop_fails(
    tmp_path: Path,
) -> None:
    process = _ProcessWithoutKillFake(alive=True)
    fixture = _registered_fixture(tmp_path, process=process)

    with pytest.raises(RuntimeError, match="does not support kill"):
        fixture.handler.request_cancellation(JOB_ID)

    assert fixture.status_publisher.cancelled == []
    assert fixture.submitted_ack.ack_count == 0
    assert fixture.record.terminal_status_claimed is True
    assert fixture.record.terminal_status == WorkerJobStatus.CANCELLED
    assert fixture.record.terminal_status_published is False
    assert fixture.record.acknowledgement_done is False
    assert fixture.record.cleanup_ready is False
    assert fixture.registry.get(JOB_ID) is fixture.record


def test_request_cancellation_does_not_ack_original_message_when_publish_cancelled_fails(
    tmp_path: Path,
) -> None:
    status_publisher = _StatusPublisherSpy(
        publish_cancelled_error=RuntimeError("publish failed")
    )
    fixture = _registered_fixture(
        tmp_path,
        status_publisher=status_publisher,
    )

    with pytest.raises(RuntimeError, match="publish failed"):
        fixture.handler.request_cancellation(JOB_ID)

    assert fixture.submitted_ack.ack_count == 0
    assert fixture.record.terminal_status_claimed is True
    assert fixture.record.terminal_status == WorkerJobStatus.CANCELLED
    assert fixture.record.terminal_status_published is False
    assert fixture.record.acknowledgement_done is False
    assert fixture.record.cleanup_ready is False
    assert fixture.registry.get(JOB_ID) is fixture.record


def test_request_cancellation_does_not_mark_cleanup_ready_when_publish_cancelled_fails(
    tmp_path: Path,
) -> None:
    status_publisher = _StatusPublisherSpy(
        publish_cancelled_error=RuntimeError("publish failed")
    )
    fixture = _registered_fixture(
        tmp_path,
        status_publisher=status_publisher,
    )

    with pytest.raises(RuntimeError, match="publish failed"):
        fixture.handler.request_cancellation(JOB_ID)

    assert fixture.record.terminal_status_published is False
    assert fixture.record.acknowledgement_done is False
    assert fixture.record.cleanup_ready is False


def test_request_cancellation_does_not_mark_cleanup_ready_when_submitted_ack_fails(
    tmp_path: Path,
) -> None:
    submitted_ack = _SubmittedAckSpy(ack_error=RuntimeError("ack failed"))
    fixture = _registered_fixture(
        tmp_path,
        submitted_ack=submitted_ack,
    )

    with pytest.raises(RuntimeError, match="ack failed"):
        fixture.handler.request_cancellation(JOB_ID)

    assert submitted_ack.ack_count == 1
    assert fixture.record.terminal_status_claimed is True
    assert fixture.record.terminal_status == WorkerJobStatus.CANCELLED
    assert fixture.record.terminal_status_published is True
    assert fixture.record.acknowledgement_done is False
    assert fixture.record.cleanup_ready is False
    assert fixture.registry.get(JOB_ID) is fixture.record


def test_request_cancellation_marks_terminal_published_only_after_publish_cancelled_succeeds(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    registry = _RecordingExecutionRegistry(events)
    status_publisher = _StatusPublisherSpy(events=events)
    submitted_ack = _SubmittedAckSpy(events=events)
    record = _record(
        tmp_path,
        process=_ProcessFake(events=events),
        parent_connection=_ParentConnectionFake(events=events),
        submitted_ack=submitted_ack,
    )
    registry.add(record)

    handler = _handler(
        execution_registry=registry,
        status_publisher=status_publisher,
    )

    handler.request_cancellation(JOB_ID)

    assert events.index("publish_cancelled") < events.index("mark_terminal_published")


def test_request_cancellation_marks_acknowledged_only_after_submitted_ack_succeeds(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    registry = _RecordingExecutionRegistry(events)
    status_publisher = _StatusPublisherSpy(events=events)
    submitted_ack = _SubmittedAckSpy(events=events)
    record = _record(
        tmp_path,
        process=_ProcessFake(events=events),
        parent_connection=_ParentConnectionFake(events=events),
        submitted_ack=submitted_ack,
    )
    registry.add(record)

    handler = _handler(
        execution_registry=registry,
        status_publisher=status_publisher,
    )

    handler.request_cancellation(JOB_ID)

    assert events.index("ack") < events.index("mark_acknowledged")


def test_request_cancellation_marks_cleanup_ready_only_after_publish_and_ack_complete(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    registry = _RecordingExecutionRegistry(events)
    status_publisher = _StatusPublisherSpy(events=events)
    submitted_ack = _SubmittedAckSpy(events=events)
    record = _record(
        tmp_path,
        process=_ProcessFake(events=events),
        parent_connection=_ParentConnectionFake(events=events),
        submitted_ack=submitted_ack,
    )
    registry.add(record)

    handler = _handler(
        execution_registry=registry,
        status_publisher=status_publisher,
    )

    handler.request_cancellation(JOB_ID)

    assert events.index("publish_cancelled") < events.index("mark_cleanup_ready")
    assert events.index("ack") < events.index("mark_cleanup_ready")
    assert events.index("mark_acknowledged") < events.index("mark_cleanup_ready")


def test_request_cancellation_does_not_publish_done(tmp_path: Path) -> None:
    fixture = _registered_fixture(tmp_path)

    fixture.handler.request_cancellation(JOB_ID)

    assert fixture.status_publisher.done == []


def test_request_cancellation_does_not_publish_error(tmp_path: Path) -> None:
    fixture = _registered_fixture(tmp_path)

    fixture.handler.request_cancellation(JOB_ID)

    assert fixture.status_publisher.error == []


def test_request_cancellation_does_not_remove_record_from_registry(
    tmp_path: Path,
) -> None:
    fixture = _registered_fixture(tmp_path)

    fixture.handler.request_cancellation(JOB_ID)

    assert fixture.registry.size() == 1
    assert fixture.registry.get(JOB_ID) is fixture.record


def test_request_cancellation_does_not_delete_local_workspace(tmp_path: Path) -> None:
    workspace = tmp_path / "jobs" / str(USER_ID) / JOB_ID
    workspace.mkdir(parents=True)
    fixture = _registered_fixture(tmp_path, workspace=workspace)

    fixture.handler.request_cancellation(JOB_ID)

    assert workspace.exists()
    assert workspace.is_dir()


def test_request_cancellation_ignores_terminal_claimed_record_without_terminal_status(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    status_publisher = _StatusPublisherSpy()
    process = _ProcessFake(alive=True)
    submitted_ack = _SubmittedAckSpy()
    record = _record(
        tmp_path,
        process=process,
        submitted_ack=submitted_ack,
    )

    record.terminal_status_claimed = True
    record.terminal_status = None

    registry.add(record)

    handler = _handler(
        execution_registry=registry,
        status_publisher=status_publisher,
    )

    handler.request_cancellation(JOB_ID)

    assert process.terminate_count == 0
    assert process.kill_count == 0
    assert process.join_timeouts == []
    assert submitted_ack.ack_count == 0
    assert status_publisher.cancelled == []
    assert status_publisher.done == []
    assert status_publisher.error == []

    assert record.terminal_status_claimed is True
    assert record.terminal_status is None
    assert record.terminal_status_published is False
    assert record.acknowledgement_done is False
    assert record.cleanup_ready is False
    assert registry.get(JOB_ID) is record


def test_request_cancellation_continues_when_parent_connection_close_fails(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    status_publisher = _StatusPublisherSpy()
    submitted_ack = _SubmittedAckSpy()
    parent_connection = _ParentConnectionFake(close_error=RuntimeError("close failed"))
    process = _ProcessFake(alive=True, terminate_stops=True)
    record = _record(
        tmp_path,
        process=process,
        parent_connection=parent_connection,
        submitted_ack=submitted_ack,
    )
    registry.add(record)

    handler = _handler(
        execution_registry=registry,
        status_publisher=status_publisher,
    )

    handler.request_cancellation(JOB_ID)

    assert process.terminate_count == 1
    assert process.kill_count == 0
    assert process.join_timeouts == [JOIN_TIMEOUT_SECONDS]

    assert parent_connection.close_count == 1

    assert len(status_publisher.cancelled) == 1
    assert status_publisher.cancelled[0]["job_id"] == JOB_ID
    assert status_publisher.cancelled[0]["worker_id"] == WORKER_ID

    assert submitted_ack.ack_count == 1

    assert record.terminal_status_claimed is True
    assert record.terminal_status == WorkerJobStatus.CANCELLED
    assert record.terminal_status_published is True
    assert record.acknowledgement_done is True
    assert record.cleanup_ready is True
    assert registry.get(JOB_ID) is record


def _handler(
    *,
    execution_registry: ExecutionRegistry | None = None,
    status_publisher: _StatusPublisherSpy | None = None,
) -> CancellationRequestHandler:
    return CancellationRequestHandler(
        execution_registry=execution_registry or ExecutionRegistry(),
        status_publisher=cast(
            StatusPublisher, status_publisher or _StatusPublisherSpy()
        ),
        worker_id=WORKER_ID,
        cancelled_process_join_timeout_seconds=JOIN_TIMEOUT_SECONDS,
        clock=_clock,
    )


def _registered_fixture(
    tmp_path: Path,
    *,
    process: Any | None = None,
    status_publisher: _StatusPublisherSpy | None = None,
    submitted_ack: "_SubmittedAckSpy | None" = None,
    workspace: Path | None = None,
) -> "_RegisteredFixture":
    registry = ExecutionRegistry()
    resolved_status_publisher = status_publisher or _StatusPublisherSpy()
    resolved_submitted_ack = submitted_ack or _SubmittedAckSpy()

    record = _record(
        tmp_path,
        process=process or _ProcessFake(),
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
        status_publisher=resolved_status_publisher,
        submitted_ack=resolved_submitted_ack,
        record=record,
        handler=handler,
    )


def _record(
    tmp_path: Path,
    *,
    job_id: str = JOB_ID,
    process: Any | None = None,
    parent_connection: "_ParentConnectionFake | None" = None,
    submitted_ack: "_SubmittedAckSpy | None" = None,
    workspace: Path | None = None,
) -> ExecutionRecord:
    work_dir = workspace or (tmp_path / "jobs" / str(USER_ID) / job_id)
    context = MagicMock()
    context.work_dir = work_dir
    context.user_id = USER_ID
    context.job_id = job_id
    context.job_type = JOB_TYPE

    return ExecutionRecord(
        job_id=job_id,
        user_id=USER_ID,
        job_type=JOB_TYPE,
        worker_id=WORKER_ID,
        manifest_object_key=f"jobs/{USER_ID}/{job_id}/manifest.json",
        manifest=MagicMock(),
        context=context,
        process=process or _ProcessFake(),
        parent_connection=parent_connection or _ParentConnectionFake(),
        submitted_ack=submitted_ack or _SubmittedAckSpy(),
        started_at=FIXED_NOW,
    )


def _clock() -> datetime:
    return FIXED_NOW


class _RegisteredFixture:
    def __init__(
        self,
        *,
        registry: ExecutionRegistry,
        status_publisher: "_StatusPublisherSpy",
        submitted_ack: "_SubmittedAckSpy",
        record: ExecutionRecord,
        handler: CancellationRequestHandler,
    ) -> None:
        self.registry = registry
        self.status_publisher = status_publisher
        self.submitted_ack = submitted_ack
        self.record = record
        self.handler = handler


class _RecordingExecutionRegistry(ExecutionRegistry):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self._events = events

    def mark_terminal_published(self, job_id: str) -> None:
        self._events.append("mark_terminal_published")
        super().mark_terminal_published(job_id)

    def mark_acknowledged(self, job_id: str) -> None:
        self._events.append("mark_acknowledged")
        super().mark_acknowledged(job_id)

    def mark_cleanup_ready(self, job_id: str) -> None:
        self._events.append("mark_cleanup_ready")
        super().mark_cleanup_ready(job_id)


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
        user_id: int,
        job_id: str,
        job_type: str,
        worker_id: str,
        message: str,
    ) -> None:
        if self._events is not None:
            self._events.append("publish_cancelled")

        if self._publish_cancelled_error is not None:
            raise self._publish_cancelled_error

        self.cancelled.append(
            {
                "user_id": user_id,
                "job_id": job_id,
                "job_type": job_type,
                "worker_id": worker_id,
                "message": message,
            }
        )

    def publish_done(
        self,
        user_id: int,
        job_id: str,
        job_type: str,
        worker_id: str,
        message: str = "",
    ) -> None:
        self.done.append(
            {
                "user_id": user_id,
                "job_id": job_id,
                "job_type": job_type,
                "worker_id": worker_id,
                "message": message,
            }
        )

    def publish_error(
        self,
        user_id: int,
        job_id: str,
        job_type: str,
        worker_id: str,
        message: str,
    ) -> None:
        self.error.append(
            {
                "user_id": user_id,
                "job_id": job_id,
                "job_type": job_type,
                "worker_id": worker_id,
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

# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import multiprocessing
from pathlib import Path
import threading
import time
from typing import Any

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.execution.artifacts import (
    InputArtifacts,
    JobParameters,
    OutputArtifacts,
    PreparedInputArtifact,
    PreparedOutputArtifact,
)
from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.models import ExecutionRecord, WorkerJobStatus
from mdds_worker_runtime.execution.registry import ExecutionRegistry
from mdds_worker_runtime.execution.timeout_watcher import TimeoutWatcher

WORKER_ID = "worker-1"
USER_ID = 42
JOB_TIMEOUT_SECONDS = 1.0
POLL_INTERVAL_SECONDS = 0.01
PROCESS_JOIN_TIMEOUT_SECONDS = 1.0
FIXED_NOW = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

TIMED_OUT_STARTED_AT = FIXED_NOW - timedelta(seconds=JOB_TIMEOUT_SECONDS + 10.0)
RUNNING_STARTED_AT = FIXED_NOW - timedelta(seconds=0.1)

TIMED_OUT_JOB_COUNT = 2
RUNNING_JOB_COUNT = 2
CLEANUP_READY_JOB_COUNT = 2


def test_timeout_watcher_finalizes_only_timed_out_running_executions(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    status_publisher = _StatusPublisherSpy()
    process_context = _multiprocessing_context()
    fixtures: list[_RecordFixture] = []

    for index in range(TIMED_OUT_JOB_COUNT):
        fixtures.append(
            _start_child_process_and_register_execution(
                tmp_path=tmp_path,
                process_context=process_context,
                registry=registry,
                job_id=f"timed-out-job-{index}",
                lifecycle_state=_LifecycleState.TIMED_OUT_RUNNING,
            )
        )

    for index in range(RUNNING_JOB_COUNT):
        fixtures.append(
            _start_child_process_and_register_execution(
                tmp_path=tmp_path,
                process_context=process_context,
                registry=registry,
                job_id=f"running-job-{index}",
                lifecycle_state=_LifecycleState.RUNNING_NOT_TIMED_OUT,
            )
        )

    for index in range(CLEANUP_READY_JOB_COUNT):
        fixtures.append(
            _start_child_process_and_register_execution(
                tmp_path=tmp_path,
                process_context=process_context,
                registry=registry,
                job_id=f"cleanup-ready-job-{index}",
                lifecycle_state=_LifecycleState.CLEANUP_READY_ERROR,
            )
        )

    watcher = TimeoutWatcher(
        execution_registry=registry,
        status_publisher=status_publisher,
        worker_id=WORKER_ID,
        job_timeout_seconds=JOB_TIMEOUT_SECONDS,
        poll_interval_seconds=POLL_INTERVAL_SECONDS,
        terminated_process_join_timeout_seconds=PROCESS_JOIN_TIMEOUT_SECONDS,
        clock=lambda: FIXED_NOW,
    )

    watcher.start()

    try:
        _wait_until(
            lambda: _assert_timed_out_jobs_finalized(
                fixtures=fixtures,
                status_publisher=status_publisher,
            ),
            timeout_seconds=2.0,
            interval_seconds=0.01,
        )

        watcher.stop(timeout_seconds=1.0)

        timed_out_job_ids = {f"timed-out-job-{index}" for index in range(2)}
        running_job_ids = {f"running-job-{index}" for index in range(2)}
        cleanup_ready_job_ids = {f"cleanup-ready-job-{index}" for index in range(2)}

        assert set(status_publisher.error_job_ids()) == timed_out_job_ids
        assert all(
            "Job execution exceeded runtime timeout and was terminated"
            in status.message
            for status in status_publisher.error_statuses()
        )

        for fixture in fixtures:
            record = fixture.record

            assert registry.get(record.job_id) is record
            assert fixture.context.work_dir.exists()
            assert fixture.context.input_dir.exists()
            assert fixture.context.output_dir.exists()

            if fixture.lifecycle_state == _LifecycleState.TIMED_OUT_RUNNING:
                assert record.job_id in timed_out_job_ids
                assert record.terminal_status_claimed is True
                assert record.terminal_status == WorkerJobStatus.ERROR
                assert record.terminal_status_published is True
                assert record.acknowledgement_done is True
                assert record.cleanup_ready is True
                assert fixture.submitted_ack.ack_count == 1
                assert not fixture.process.is_alive()

            if fixture.lifecycle_state == _LifecycleState.RUNNING_NOT_TIMED_OUT:
                assert record.job_id in running_job_ids
                assert record.terminal_status_claimed is False
                assert record.terminal_status is None
                assert record.terminal_status_published is False
                assert record.acknowledgement_done is False
                assert record.cleanup_ready is False
                assert fixture.submitted_ack.ack_count == 0
                assert fixture.process.is_alive()

            if fixture.lifecycle_state == _LifecycleState.CLEANUP_READY_ERROR:
                assert record.job_id in cleanup_ready_job_ids
                assert record.terminal_status_claimed is True
                assert record.terminal_status == WorkerJobStatus.ERROR
                assert record.terminal_status_published is True
                assert record.acknowledgement_done is True
                assert record.cleanup_ready is True
                assert fixture.submitted_ack.ack_count == 1
                assert fixture.process.is_alive()

        assert registry.size() == len(fixtures)

    finally:
        watcher.stop(timeout_seconds=1.0)
        _stop_child_processes(fixtures)


def _start_child_process_and_register_execution(
    *,
    tmp_path: Path,
    process_context,
    registry: ExecutionRegistry,
    job_id: str,
    lifecycle_state: "_LifecycleState",
) -> "_RecordFixture":
    parent_connection, child_connection = process_context.Pipe(duplex=False)

    process = process_context.Process(
        target=_sleep_until_terminated,
        args=(60.0,),
    )
    process.start()
    child_connection.close()

    submitted_ack = _SubmittedAckSpy()
    context = _context(tmp_path, job_id=job_id)
    _create_local_workspace(context)

    record = ExecutionRecord(
        job_id=job_id,
        user_id=context.user_id,
        job_type=context.job_type,
        worker_id=WORKER_ID,
        manifest_object_key=f"jobs/{context.user_id}/{job_id}/manifest.json",
        manifest=None,
        context=context,
        process=process,
        parent_connection=parent_connection,
        submitted_ack=submitted_ack,
        started_at=_started_at_for(lifecycle_state),
    )

    registry.add(record)
    _apply_lifecycle_state(
        registry=registry,
        record=record,
        submitted_ack=submitted_ack,
        lifecycle_state=lifecycle_state,
    )

    return _RecordFixture(
        record=record,
        submitted_ack=submitted_ack,
        lifecycle_state=lifecycle_state,
        context=context,
        parent_connection=parent_connection,
        process=process,
    )


def _sleep_until_terminated(delay_seconds: float) -> None:
    time.sleep(delay_seconds)


def _started_at_for(lifecycle_state: "_LifecycleState") -> datetime:
    if lifecycle_state == _LifecycleState.RUNNING_NOT_TIMED_OUT:
        return RUNNING_STARTED_AT

    return TIMED_OUT_STARTED_AT


def _apply_lifecycle_state(
    *,
    registry: ExecutionRegistry,
    record: ExecutionRecord,
    submitted_ack: "_SubmittedAckSpy",
    lifecycle_state: "_LifecycleState",
) -> None:
    if lifecycle_state in {
        _LifecycleState.TIMED_OUT_RUNNING,
        _LifecycleState.RUNNING_NOT_TIMED_OUT,
    }:
        return

    if lifecycle_state == _LifecycleState.CLEANUP_READY_ERROR:
        claimed = registry.try_claim_terminal(
            job_id=record.job_id,
            terminal_status=WorkerJobStatus.ERROR,
            message="RuntimeError: execution already failed.",
            finished_at=FIXED_NOW,
        )
        assert claimed is record

        registry.mark_terminal_published(record.job_id)
        submitted_ack.ack()
        registry.mark_acknowledged(record.job_id)
        registry.mark_cleanup_ready(record.job_id)
        return

    raise AssertionError(f"Unsupported lifecycle state: {lifecycle_state}")


def _assert_timed_out_jobs_finalized(
    *,
    fixtures: list["_RecordFixture"],
    status_publisher: "_StatusPublisherSpy",
) -> None:
    assert len(status_publisher.error_job_ids()) == TIMED_OUT_JOB_COUNT

    timed_out_fixtures = [
        fixture
        for fixture in fixtures
        if fixture.lifecycle_state == _LifecycleState.TIMED_OUT_RUNNING
    ]

    assert len(timed_out_fixtures) == TIMED_OUT_JOB_COUNT

    for fixture in timed_out_fixtures:
        record = fixture.record

        assert record.terminal_status_claimed is True
        assert record.terminal_status == WorkerJobStatus.ERROR
        assert record.terminal_status_published is True
        assert record.acknowledgement_done is True
        assert record.cleanup_ready is True
        assert fixture.submitted_ack.ack_count == 1
        assert not fixture.process.is_alive()


def _create_local_workspace(context: JobExecutionContext) -> None:
    context.input_dir.mkdir(parents=True)
    context.output_dir.mkdir(parents=True)

    context.input_path("matrix").write_text(
        "1,0\n0,1\n",
        encoding="utf-8",
    )
    context.input_path("rhs").write_text(
        "7\n11\n",
        encoding="utf-8",
    )
    context.output_path("solution").write_text(
        "7\n11\n",
        encoding="utf-8",
    )


def _wait_until(
    assertion,
    *,
    timeout_seconds: float,
    interval_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: AssertionError | None = None

    while time.monotonic() < deadline:
        try:
            assertion()
            return
        except AssertionError as exc:
            last_error = exc
            time.sleep(interval_seconds)

    if last_error is not None:
        raise last_error

    raise AssertionError("Condition was not satisfied before timeout.")


def _stop_child_processes(fixtures: list["_RecordFixture"]) -> None:
    for fixture in fixtures:
        fixture.process.join(timeout=1.0)

        if fixture.process.is_alive():
            fixture.process.terminate()
            fixture.process.join(timeout=1.0)

        try:
            fixture.parent_connection.close()
        except OSError:
            pass


def _multiprocessing_context():
    try:
        return multiprocessing.get_context("fork")
    except ValueError:
        return multiprocessing.get_context()


def _context(tmp_path: Path, *, job_id: str) -> JobExecutionContext:
    work_dir = tmp_path / "jobs" / str(USER_ID) / job_id
    input_dir = work_dir / "in"
    output_dir = work_dir / "out"

    return JobExecutionContext(
        user_id=USER_ID,
        job_id=job_id,
        job_type="SOLVING_SLAE",
        work_dir=work_dir,
        input_dir=input_dir,
        output_dir=output_dir,
        inputs=InputArtifacts(
            {
                "matrix": PreparedInputArtifact(
                    object_key=f"jobs/{USER_ID}/{job_id}/in/matrix.csv",
                    local_path=input_dir / "matrix.csv",
                    format=ArtifactFormat.CSV,
                ),
                "rhs": PreparedInputArtifact(
                    object_key=f"jobs/{USER_ID}/{job_id}/in/rhs.csv",
                    local_path=input_dir / "rhs.csv",
                    format=ArtifactFormat.CSV,
                ),
            }
        ),
        outputs=OutputArtifacts(
            {
                "solution": PreparedOutputArtifact(
                    object_key=f"jobs/{USER_ID}/{job_id}/out/solution.csv",
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
    submitted_ack: "_SubmittedAckSpy"
    lifecycle_state: "_LifecycleState"
    context: JobExecutionContext
    parent_connection: Any
    process: Any


@dataclass(frozen=True)
class _PublishedStatus:
    job_id: str
    user_id: int
    job_type: str
    worker_id: str
    message: str


class _StatusPublisherSpy:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._error: list[_PublishedStatus] = []

    def publish_error(
        self,
        user_id: int,
        job_id: str,
        job_type: str,
        worker_id: str,
        message: str,
    ) -> None:
        with self._lock:
            self._error.append(
                _PublishedStatus(
                    job_id=job_id,
                    user_id=user_id,
                    job_type=job_type,
                    worker_id=worker_id,
                    message=message,
                )
            )

    def error_statuses(self) -> list[_PublishedStatus]:
        with self._lock:
            return list(self._error)

    def error_job_ids(self) -> list[str]:
        with self._lock:
            return [status.job_id for status in self._error]


class _SubmittedAckSpy:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ack_count = 0

    def ack(self) -> None:
        with self._lock:
            self._ack_count += 1

    @property
    def ack_count(self) -> int:
        with self._lock:
            return self._ack_count


class _LifecycleState:
    TIMED_OUT_RUNNING = "TIMED_OUT_RUNNING"
    RUNNING_NOT_TIMED_OUT = "RUNNING_NOT_TIMED_OUT"
    CLEANUP_READY_ERROR = "CLEANUP_READY_ERROR"

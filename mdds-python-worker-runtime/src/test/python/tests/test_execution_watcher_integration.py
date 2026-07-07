# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import multiprocessing
from pathlib import Path
import threading
import time
from typing import Any, cast

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.domain.manifest import ArtifactRef, JobManifest
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

WORKER_ID = "worker-1"
JOB_TIMEOUT_SECONDS = 1.0
POLL_INTERVAL_SECONDS = 0.01
PROGRESS_INTERVAL_SECONDS = 0.02
SUCCEEDED_JOB_COUNT = 5
FAILED_JOB_COUNT = 5


def test_execution_watcher_finalizes_multiple_real_child_processes(
    tmp_path: Path,
) -> None:
    registry = ExecutionRegistry()
    coordinator = JobStateTransitionCoordinator()
    status_publisher = _StatusPublisherSpy()
    output_artifact_uploader = _OutputArtifactUploaderSpy()

    process_context = _multiprocessing_context()
    fixtures: list[_RecordFixture] = []

    for index in range(SUCCEEDED_JOB_COUNT):
        job_id = f"succeeded-job-{index}"
        fixture = _start_child_process_and_register_execution(
            tmp_path=tmp_path,
            process_context=process_context,
            registry=registry,
            job_state_transition_coordinator=coordinator,
            job_id=job_id,
            should_succeed=True,
            delay_seconds=0.01 * (index + 1),
        )
        fixtures.append(fixture)

    for index in range(FAILED_JOB_COUNT):
        job_id = f"failed-job-{index}"
        fixture = _start_child_process_and_register_execution(
            tmp_path=tmp_path,
            process_context=process_context,
            registry=registry,
            job_state_transition_coordinator=coordinator,
            job_id=job_id,
            should_succeed=False,
            delay_seconds=0.01 * (SUCCEEDED_JOB_COUNT + index + 1),
        )
        fixtures.append(fixture)

    watcher = ExecutionWatcher(
        execution_registry=registry,
        job_state_transition_coordinator=coordinator,
        output_artifact_uploader=output_artifact_uploader,
        status_publisher=status_publisher,
        worker_id=WORKER_ID,
        job_timeout_seconds=JOB_TIMEOUT_SECONDS,
        poll_interval_seconds=POLL_INTERVAL_SECONDS,
        progress_interval_seconds=PROGRESS_INTERVAL_SECONDS,
        clock=lambda: datetime.now(timezone.utc),
    )

    watcher.start()

    try:
        _wait_until(
            lambda: _assert_all_jobs_finalized(
                fixtures=fixtures,
                job_state_transition_coordinator=coordinator,
                status_publisher=status_publisher,
            ),
            timeout_seconds=2.0,
            interval_seconds=0.01,
        )
    finally:
        watcher.stop(timeout_seconds=1.0)
        _stop_child_processes(fixtures)

    succeeded_job_ids = {f"succeeded-job-{index}" for index in range(5)}
    failed_job_ids = {f"failed-job-{index}" for index in range(5)}

    assert set(status_publisher.done_job_ids()) == succeeded_job_ids
    assert set(status_publisher.error_job_ids()) == failed_job_ids

    assert output_artifact_uploader.uploaded_job_ids() == sorted(succeeded_job_ids)

    for fixture in fixtures:
        record = fixture.record
        state = coordinator.get_state(record.job_id)

        assert fixture.submitted_ack.ack_count == 1

        if fixture.should_succeed:
            assert state is WorkerJobStatus.DONE
            assert record.job_id in succeeded_job_ids
            assert record.job_id not in failed_job_ids
        else:
            assert state is WorkerJobStatus.ERROR
            assert record.job_id in failed_job_ids
            assert record.job_id not in succeeded_job_ids


def _start_child_process_and_register_execution(
    *,
    tmp_path: Path,
    process_context,
    registry: ExecutionRegistry,
    job_state_transition_coordinator: JobStateTransitionCoordinator,
    job_id: str,
    should_succeed: bool,
    delay_seconds: float,
) -> "_RecordFixture":
    parent_connection, child_connection = process_context.Pipe(duplex=False)

    process = process_context.Process(
        target=_send_supervised_execution_result,
        args=(child_connection, job_id, should_succeed, delay_seconds),
    )
    process.start()
    child_connection.close()

    submitted_ack = _SubmittedAckSpy()
    context = _context(tmp_path, job_id=job_id)

    execution_record = ExecutionRecord(
        workspace=context.workspace,
        context=context,
        process_record=ProcessRecord(
            process=process,
            parent_connection=parent_connection,
            started_at=datetime.now(timezone.utc),
        ),
    )

    registry.add(execution_record)
    record = job_state_transition_coordinator.create(
        job_id, cast(Acknowledger, cast(object, submitted_ack))
    )
    with record.lock:
        record.state = WorkerJobStatus.IN_PROGRESS

    return _RecordFixture(
        record=execution_record,
        submitted_ack=submitted_ack,
        should_succeed=should_succeed,
        parent_connection=parent_connection,
        process=process,
    )


def _send_supervised_execution_result(
    child_connection,
    job_id: str,
    should_succeed: bool,
    delay_seconds: float,
) -> None:
    try:
        time.sleep(delay_seconds)

        if should_succeed:
            child_connection.send(SupervisedExecutionResult.succeeded(job_id))
            return

        child_connection.send(
            SupervisedExecutionResult(
                job_id=job_id,
                status=SupervisedExecutionStatus.FAILED,
                message=f"Execution failed for {job_id}.",
                error_type="RuntimeError",
            )
        )
    finally:
        child_connection.close()


def _assert_all_jobs_finalized(
    *,
    fixtures: list["_RecordFixture"],
    job_state_transition_coordinator: JobStateTransitionCoordinator,
    status_publisher: "_StatusPublisherSpy",
) -> None:
    assert len(status_publisher.done_job_ids()) == SUCCEEDED_JOB_COUNT
    assert len(status_publisher.error_job_ids()) == FAILED_JOB_COUNT

    assert (
        sum(
            1
            for fixture in fixtures
            if job_state_transition_coordinator.get_state(fixture.record.job_id)
            in {
                WorkerJobStatus.DONE,
                WorkerJobStatus.ERROR,
            }
        )
        == SUCCEEDED_JOB_COUNT + FAILED_JOB_COUNT
    )
    assert sum(fixture.submitted_ack.ack_count for fixture in fixtures) == (
        SUCCEEDED_JOB_COUNT + FAILED_JOB_COUNT
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

        fixture.parent_connection.close()


def _multiprocessing_context():
    try:
        return multiprocessing.get_context("fork")
    except ValueError:
        return multiprocessing.get_context()


def _context(tmp_path: Path, *, job_id: str) -> JobExecutionContext:
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
        worker_id=WORKER_ID,
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
    submitted_ack: "_SubmittedAckSpy"
    should_succeed: bool
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
        self._done: list[_PublishedStatus] = []
        self._error: list[_PublishedStatus] = []
        self._in_progress: list[_PublishedStatus] = []

    def publish_done(
        self,
        workspace: JobWorkspace,
        message: str = "Job completed successfully.",
    ) -> None:
        with self._lock:
            self._done.append(
                _PublishedStatus(
                    job_id=workspace.job_id,
                    user_id=workspace.user_id,
                    job_type=workspace.job_type,
                    worker_id=workspace.worker_id,
                    message=message,
                )
            )

    def publish_error(
        self,
        workspace: JobWorkspace,
        message: str,
    ) -> None:
        with self._lock:
            self._error.append(
                _PublishedStatus(
                    job_id=workspace.job_id,
                    user_id=workspace.user_id,
                    job_type=workspace.job_type,
                    worker_id=workspace.worker_id,
                    message=message,
                )
            )

    def publish_in_progress(
        self,
        workspace: JobWorkspace,
        progress: int,
        message: str,
    ) -> None:
        del progress

        with self._lock:
            self._in_progress.append(
                _PublishedStatus(
                    job_id=workspace.job_id,
                    user_id=workspace.user_id,
                    job_type=workspace.job_type,
                    worker_id=workspace.worker_id,
                    message=message,
                )
            )

    def done_job_ids(self) -> list[str]:
        with self._lock:
            return [status.job_id for status in self._done]

    def error_job_ids(self) -> list[str]:
        with self._lock:
            return [status.job_id for status in self._error]

    def in_progress_job_ids(self) -> list[str]:
        with self._lock:
            return [status.job_id for status in self._in_progress]


class _OutputArtifactUploaderSpy:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._uploaded_job_ids: list[str] = []

    def upload(self, context: JobExecutionContext) -> None:
        with self._lock:
            self._uploaded_job_ids.append(context.job_id)

    def uploaded_job_ids(self) -> list[str]:
        with self._lock:
            return sorted(self._uploaded_job_ids)


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

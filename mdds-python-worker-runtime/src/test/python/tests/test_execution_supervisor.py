# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from dataclasses import replace
from datetime import timezone
from unittest.mock import MagicMock

import pytest

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
from mdds_worker_runtime.execution.context_snapshot import (
    JobExecutionContextSnapshotStore,
)
from mdds_worker_runtime.execution.supervised_process import run_job_in_child_process
from mdds_worker_runtime.execution.supervisor import (
    CONTEXT_SNAPSHOT_FILE_NAME,
    ExecutionSupervisor,
    ExecutionSupervisorStartError,
    SupervisedExecutionRequest,
)
from mdds_worker_runtime.execution.workspace import JobWorkspace


def test_execution_supervisor_saves_snapshot_starts_process_and_returns_record(
    tmp_path,
) -> None:
    context = _create_context(tmp_path)
    snapshot_store = MagicMock(spec=JobExecutionContextSnapshotStore)
    process_context = _FakeProcessContext()

    supervisor = ExecutionSupervisor(
        handler_import_path="tests.fixtures.job_handlers:WritingExecuteJobHandler",
        snapshot_store=snapshot_store,
        process_context=process_context,
    )

    request = SupervisedExecutionRequest(
        context=context,
    )

    record = supervisor.start(request)

    expected_snapshot_path = (
        tmp_path / "jobs" / "42" / "job-1" / CONTEXT_SNAPSHOT_FILE_NAME
    )

    snapshot_store.save.assert_called_once_with(context, expected_snapshot_path)

    assert process_context.pipe_duplex is False
    assert process_context.process.target is run_job_in_child_process
    assert process_context.process.args == (
        "job-1",
        "tests.fixtures.job_handlers:WritingExecuteJobHandler",
        expected_snapshot_path,
        process_context.child_connection,
    )
    assert process_context.process.name == "mdds-job-job-1"
    assert process_context.process.started is True
    assert process_context.child_connection.closed is True

    assert record.process is process_context.process
    assert record.parent_connection is process_context.parent_connection
    assert record.started_at.tzinfo == timezone.utc
    assert record.finished_at is None


def test_execution_supervisor_rejects_null_request(tmp_path) -> None:
    supervisor = ExecutionSupervisor(
        handler_import_path="tests.fixtures.job_handlers:WritingExecuteJobHandler",
        process_context=_FakeProcessContext(),
    )

    with pytest.raises(ValueError, match="request cannot be null."):
        supervisor.start(None)  # type: ignore[arg-type]


def test_execution_supervisor_closes_connections_when_process_start_fails(
    tmp_path,
) -> None:
    context = _create_context(tmp_path)
    snapshot_store = MagicMock(spec=JobExecutionContextSnapshotStore)
    process_context = _FakeProcessContext(
        process=_FakeProcess(start_error=RuntimeError("boom")),
    )

    supervisor = ExecutionSupervisor(
        handler_import_path="tests.fixtures.job_handlers:WritingExecuteJobHandler",
        snapshot_store=snapshot_store,
        process_context=process_context,
    )

    request = SupervisedExecutionRequest(
        context=context,
    )

    with pytest.raises(
        ExecutionSupervisorStartError,
        match="Cannot start supervised execution process for job job-1.",
    ):
        supervisor.start(request)

    assert process_context.parent_connection.closed is True
    assert process_context.child_connection.closed is True


class _FakeConnection:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeProcess:
    def __init__(self, start_error: BaseException | None = None) -> None:
        self.start_error = start_error
        self.started = False
        self.target = None
        self.args = None
        self.name = None

    def configure(self, target, args, name: str) -> None:
        self.target = target
        self.args = args
        self.name = name

    def start(self) -> None:
        if self.start_error is not None:
            raise self.start_error

        self.started = True


class _FakeProcessContext:
    def __init__(self, process: _FakeProcess | None = None) -> None:
        self.parent_connection = _FakeConnection()
        self.child_connection = _FakeConnection()
        self.process = process or _FakeProcess()
        self.pipe_duplex = None

    def Pipe(self, duplex: bool):
        self.pipe_duplex = duplex
        return self.parent_connection, self.child_connection

    def Process(self, target, args, name: str):
        self.process.configure(target, args, name)
        return self.process


def _create_context(tmp_path) -> JobExecutionContext:
    work_dir = tmp_path / "jobs" / "42" / "job-1"
    input_dir = work_dir / "in"
    output_dir = work_dir / "out"

    manifest = JobManifest(
        manifest_version=1,
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        inputs={
            "matrix": ArtifactRef(
                object_key="jobs/42/job-1/in/matrix.csv",
                format=ArtifactFormat.CSV,
            ),
        },
        params={
            "solvingMethod": "numpy_exact_solver",
        },
        outputs={
            "solution": ArtifactRef(
                object_key="jobs/42/job-1/out/solution.csv",
                format=ArtifactFormat.CSV,
            ),
        },
    )

    workspace = JobWorkspace(
        manifest=manifest,
        work_dir=work_dir,
        input_dir=input_dir,
        output_dir=output_dir,
        worker_id="worker-1",
    )

    return JobExecutionContext(
        workspace=workspace,
        inputs=InputArtifacts(
            {
                "matrix": PreparedInputArtifact(
                    object_key="jobs/42/job-1/in/matrix.csv",
                    local_path=input_dir / "matrix.csv",
                    format=ArtifactFormat.CSV,
                ),
            }
        ),
        outputs=OutputArtifacts(
            {
                "solution": PreparedOutputArtifact(
                    object_key="jobs/42/job-1/out/solution.csv",
                    local_path=output_dir / "solution.csv",
                    format=ArtifactFormat.CSV,
                ),
            }
        ),
        params=JobParameters(manifest.params),
    )


def _create_supervisor(tmp_path) -> ExecutionSupervisor:
    return ExecutionSupervisor(
        handler_import_path="tests.fixtures.job_handlers:WritingExecuteJobHandler",
        process_context=_FakeProcessContext(),
    )


def _create_request(tmp_path) -> SupervisedExecutionRequest:
    return SupervisedExecutionRequest(
        context=_create_context(tmp_path),
    )


@pytest.mark.parametrize("handler_import_path", [None, "", " "])
def test_execution_supervisor_rejects_null_or_blank_handler_import_path(
    handler_import_path,
    tmp_path,
) -> None:
    with pytest.raises(
        ValueError,
        match="handler_import_path cannot be null or blank.",
    ):
        ExecutionSupervisor(
            handler_import_path=handler_import_path,  # type: ignore[arg-type]
            process_context=_FakeProcessContext(),
        )


def test_execution_supervisor_rejects_null_request_context(tmp_path) -> None:
    supervisor = _create_supervisor(tmp_path)
    request = replace(
        _create_request(tmp_path),
        context=None,  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="request context cannot be null."):
        supervisor.start(request)


def test_execution_supervisor_rejects_null_context(tmp_path) -> None:
    supervisor = _create_supervisor(tmp_path)
    request = replace(
        _create_request(tmp_path),
        context=None,  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="context cannot be null."):
        supervisor.start(request)

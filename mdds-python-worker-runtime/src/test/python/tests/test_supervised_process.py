# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

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
from mdds_worker_runtime.execution.supervised_process import (
    SupervisedExecutionStatus,
    run_job_in_child_process,
)
from mdds_worker_runtime.execution.workspace import JobWorkspace

FIXTURE_MODULE = "tests.fixtures.job_handlers"


def test_run_job_in_child_process_sends_succeeded_result(tmp_path) -> None:
    context = _create_context(tmp_path)
    snapshot_path = tmp_path / "context.snapshot.json"
    JobExecutionContextSnapshotStore().save(context, snapshot_path)

    child_connection = _FakeConnection()

    run_job_in_child_process(
        "job-1",
        f"{FIXTURE_MODULE}:WritingExecuteJobHandler",
        snapshot_path,
        child_connection,
    )

    assert len(child_connection.sent_messages) == 1

    result = child_connection.sent_messages[0]
    assert result.job_id == "job-1"
    assert result.status == SupervisedExecutionStatus.SUCCEEDED
    assert result.message == "Execution succeeded."
    assert result.error_type is None

    assert context.outputs.path("solution").read_bytes() == b"execution-result"
    assert child_connection.closed is True


def test_run_job_in_child_process_sends_failed_result_on_execute_exception(
    tmp_path,
) -> None:
    context = _create_context(tmp_path)
    snapshot_path = tmp_path / "context.snapshot.json"
    JobExecutionContextSnapshotStore().save(context, snapshot_path)

    child_connection = _FakeConnection()

    run_job_in_child_process(
        "job-1",
        f"{FIXTURE_MODULE}:FailingExecuteJobHandler",
        snapshot_path,
        child_connection,
    )

    assert len(child_connection.sent_messages) == 1

    result = child_connection.sent_messages[0]
    assert result.job_id == "job-1"
    assert result.status == SupervisedExecutionStatus.FAILED
    assert result.message == "execute failed"
    assert result.error_type == "RuntimeError"
    assert child_connection.closed is True


class _FakeConnection:
    def __init__(self) -> None:
        self.sent_messages = []
        self.closed = False

    def send(self, message) -> None:
        self.sent_messages.append(message)

    def close(self) -> None:
        self.closed = True


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

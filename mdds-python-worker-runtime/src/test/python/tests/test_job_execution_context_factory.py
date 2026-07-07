# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from pathlib import Path
from types import SimpleNamespace

import pytest

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.domain.manifest import JobManifest, ArtifactRef
from mdds_worker_runtime.execution.artifacts import (
    PreparedInputArtifact,
    PreparedJobInputs,
)
from mdds_worker_runtime.execution.context import JobExecutionContextFactory
from mdds_worker_runtime.execution.workspace import JobWorkspace


def test_context_factory_creates_context_from_manifest_and_prepared_inputs(
    tmp_path,
) -> None:
    jobs_root = tmp_path / "jobs"
    manifest = _create_manifest()
    prepared_job_inputs = _create_prepared_job_inputs(jobs_root)

    workspace = _create_workspace(jobs_root, manifest)

    context = JobExecutionContextFactory(jobs_root).create(
        workspace=workspace,
        prepared_job_inputs=prepared_job_inputs,
    )

    expected_work_dir = jobs_root / "42" / "job-1"
    expected_input_dir = expected_work_dir / "in"
    expected_output_dir = expected_work_dir / "out"

    assert context.user_id == 42
    assert context.job_id == "job-1"
    assert context.job_type == "SOLVING_SLAE"
    assert context.work_dir == expected_work_dir
    assert context.input_dir == expected_input_dir
    assert context.output_dir == expected_output_dir

    assert context.input_path("matrix") == expected_input_dir / "matrix.csv"
    assert context.input_path("rhs") == expected_input_dir / "rhs.csv"
    assert context.output_path("solution") == expected_output_dir / "solution.csv"
    assert context.required_param("solvingMethod") == "numpy_exact_solver"


def test_context_factory_creates_output_dir(tmp_path) -> None:
    jobs_root = tmp_path / "jobs"
    manifest = _create_manifest()
    workspace = _create_workspace(jobs_root, manifest)
    prepared_job_inputs = _create_prepared_job_inputs(jobs_root)

    context = JobExecutionContextFactory(jobs_root).create(
        workspace,
        prepared_job_inputs,
    )

    assert context.output_dir.is_dir()


def test_context_factory_maps_manifest_outputs_to_local_output_paths(tmp_path) -> None:
    jobs_root = tmp_path / "jobs"
    manifest = _create_manifest(
        outputs={
            "solution": _artifact_ref(
                "jobs/42/job-1/out/solution.csv", ArtifactFormat.CSV
            ),
            "metrics": _artifact_ref(
                "jobs/42/job-1/out/metrics.json", ArtifactFormat.JSON
            ),
        }
    )
    workspace = _create_workspace(jobs_root, manifest)
    prepared_job_inputs = _create_prepared_job_inputs(jobs_root)

    context = JobExecutionContextFactory(jobs_root).create(
        workspace,
        prepared_job_inputs,
    )

    expected_output_dir = jobs_root / "42" / "job-1" / "out"

    assert context.output("solution").object_key == "jobs/42/job-1/out/solution.csv"
    assert context.output("solution").local_path == expected_output_dir / "solution.csv"
    assert context.output("solution").format == ArtifactFormat.CSV

    assert context.output("metrics").object_key == "jobs/42/job-1/out/metrics.json"
    assert context.output("metrics").local_path == expected_output_dir / "metrics.json"
    assert context.output("metrics").format == ArtifactFormat.JSON


def test_context_factory_copies_manifest_params(tmp_path) -> None:
    jobs_root = tmp_path / "jobs"
    manifest = _create_manifest(
        params={
            "solvingMethod": "numpy_exact_solver",
            "tolerance": 1e-9,
        }
    )
    workspace = _create_workspace(jobs_root, manifest)
    prepared_job_inputs = _create_prepared_job_inputs(jobs_root)

    context = JobExecutionContextFactory(jobs_root).create(
        workspace,
        prepared_job_inputs,
    )

    assert context.required_param("solvingMethod") == "numpy_exact_solver"
    assert context.required_param("tolerance") == 1e-9


def test_context_factory_rejects_null_jobs_root() -> None:
    with pytest.raises(ValueError, match="jobs_root cannot be null."):
        JobExecutionContextFactory(None)


def test_context_factory_rejects_null_workspace(tmp_path) -> None:
    jobs_root = tmp_path / "jobs"
    prepared_job_inputs = _create_prepared_job_inputs(jobs_root)

    with pytest.raises(ValueError, match="workspace cannot be null."):
        JobExecutionContextFactory(jobs_root).create(
            workspace=None,
            prepared_job_inputs=prepared_job_inputs,
        )


def test_context_factory_rejects_null_prepared_inputs(tmp_path) -> None:
    jobs_root = tmp_path / "jobs"
    manifest = _create_manifest()
    workspace = _create_workspace(jobs_root, manifest)

    with pytest.raises(ValueError, match="prepared_job_inputs cannot be null."):
        JobExecutionContextFactory(jobs_root).create(
            workspace,
            None,
        )


def test_context_factory_rejects_duplicate_output_local_path(tmp_path) -> None:
    jobs_root = tmp_path / "jobs"
    manifest = _create_manifest(
        outputs={
            "solution": _artifact_ref(
                "jobs/42/job-1/out/solution.csv", ArtifactFormat.CSV
            ),
            "debugSolution": _artifact_ref(
                "jobs/42/job-1/debug/solution.csv", ArtifactFormat.CSV
            ),
        }
    )
    workspace = _create_workspace(jobs_root, manifest)
    prepared_job_inputs = _create_prepared_job_inputs(jobs_root)

    with pytest.raises(ValueError, match="Duplicate local output artifact path:"):
        JobExecutionContextFactory(jobs_root).create(
            workspace,
            prepared_job_inputs,
        )


def _create_manifest(
    user_id: int = 42,
    job_id: str = "job-1",
    job_type: str = "SOLVING_SLAE",
    outputs: dict[str, ArtifactRef] | None = None,
    params: dict[str, object] | None = None,
) -> JobManifest:
    return JobManifest(
        manifest_version=1,
        user_id=user_id,
        job_id=job_id,
        job_type=job_type,
        inputs={},
        outputs=outputs
        or {
            "solution": _artifact_ref(
                "jobs/42/job-1/out/solution.csv",
                ArtifactFormat.CSV,
            ),
        },
        params=params
        or {
            "solvingMethod": "numpy_exact_solver",
        },
    )


def _create_prepared_job_inputs(
    jobs_root: Path,
    user_id: int = 42,
    job_id: str = "job-1",
) -> PreparedJobInputs:
    input_dir = jobs_root / str(user_id) / job_id / "in"

    return PreparedJobInputs(
        inputs={
            "matrix": PreparedInputArtifact(
                object_key=f"jobs/{user_id}/{job_id}/in/matrix.csv",
                local_path=input_dir / "matrix.csv",
                format=ArtifactFormat.CSV,
            ),
            "rhs": PreparedInputArtifact(
                object_key=f"jobs/{user_id}/{job_id}/in/rhs.csv",
                local_path=input_dir / "rhs.csv",
                format=ArtifactFormat.CSV,
            ),
        },
    )


def _artifact_ref(
    object_key: str,
    artifact_format: ArtifactFormat,
) -> SimpleNamespace:
    return SimpleNamespace(
        object_key=object_key,
        format=artifact_format,
    )


def _create_workspace(
    jobs_root: Path,
    manifest: JobManifest,
) -> JobWorkspace:
    work_dir = jobs_root / str(manifest.user_id) / manifest.job_id

    return JobWorkspace(
        manifest=manifest,
        work_dir=work_dir,
        input_dir=work_dir / "in",
        output_dir=work_dir / "out",
        worker_id="worker-1",
    )

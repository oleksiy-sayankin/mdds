# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import boto3
import pytest
from botocore.config import Config
from testcontainers.minio import MinioContainer

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.domain.manifest import ArtifactRef, JobManifest
from mdds_worker_runtime.execution.artifacts import InputArtifactPreparer
from mdds_worker_runtime.execution.context import JobExecutionContextFactory
from mdds_worker_runtime.storage.s3_client import S3Storage

TEST_RESOURCES_DIR = Path(__file__).resolve().parents[2] / "resources"


def _read_test_resource(file_name: str) -> bytes:
    path = TEST_RESOURCES_DIR / file_name
    assert path.is_file(), f"Test resource does not exist: {path}"
    return path.read_bytes()


def _put_bytes(
    s3_client,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str = "text/csv",
) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )


@pytest.fixture(scope="module")
def s3_client_and_bucket():
    """Create MinIO client and bucket in test MinIO container."""
    bucket = "mdds-test"
    access_key = "minioadmin"
    secret_key = "minioadmin"

    with MinioContainer(access_key=access_key, secret_key=secret_key) as minio:
        config = minio.get_config()

        endpoint = (
            config.get("endpoint")
            if isinstance(config, dict)
            else getattr(config, "endpoint")
        )

        if not endpoint.startswith(("http://", "https://")):
            endpoint = "http://" + endpoint

        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}),
        )

        s3_client.create_bucket(Bucket=bucket)

        yield s3_client, bucket


def test_prepare_input_artifacts_downloads_files_from_s3_to_local_job_workspace(
    s3_client_and_bucket,
    tmp_path: Path,
) -> None:
    s3_client, bucket = s3_client_and_bucket

    user_id = 12345
    job_id = "job-input-artifacts-download"

    matrix_key = f"jobs/{user_id}/{job_id}/in/matrix.csv"
    rhs_key = f"jobs/{user_id}/{job_id}/in/rhs.csv"

    matrix_content = _read_test_resource("matrix.csv")
    rhs_content = _read_test_resource("rhs.csv")

    _put_bytes(
        s3_client=s3_client,
        bucket=bucket,
        key=matrix_key,
        body=matrix_content,
    )
    _put_bytes(
        s3_client=s3_client,
        bucket=bucket,
        key=rhs_key,
        body=rhs_content,
    )

    storage = S3Storage(s3_client, bucket)
    preparer = InputArtifactPreparer(storage, tmp_path)

    prepared = preparer.prepare(
        user_id=user_id,
        job_id=job_id,
        inputs={
            "matrix": ArtifactRef(
                object_key=matrix_key,
                format=ArtifactFormat.CSV,
            ),
            "rhs": ArtifactRef(
                object_key=rhs_key,
                format=ArtifactFormat.CSV,
            ),
        },
    )

    expected_input_dir = tmp_path / str(user_id) / job_id / "in"
    expected_matrix_path = expected_input_dir / "matrix.csv"
    expected_rhs_path = expected_input_dir / "rhs.csv"

    assert prepared.user_id == user_id
    assert prepared.job_id == job_id
    assert prepared.input_dir == expected_input_dir

    assert prepared.inputs["matrix"].object_key == matrix_key
    assert prepared.inputs["matrix"].local_path == expected_matrix_path

    assert prepared.inputs["rhs"].object_key == rhs_key
    assert prepared.inputs["rhs"].local_path == expected_rhs_path

    assert expected_input_dir.is_dir()

    assert expected_matrix_path.is_file()
    assert expected_rhs_path.is_file()

    assert expected_matrix_path.read_bytes() == matrix_content
    assert expected_rhs_path.read_bytes() == rhs_content


def test_prepared_inputs_are_available_through_job_execution_context_api(
    s3_client_and_bucket,
    tmp_path: Path,
) -> None:
    s3_client, bucket = s3_client_and_bucket

    user_id = 12345
    job_id = "job-context-artifact-api"

    matrix_key = f"jobs/{user_id}/{job_id}/in/matrix.csv"
    rhs_key = f"jobs/{user_id}/{job_id}/in/rhs.csv"
    solution_key = f"jobs/{user_id}/{job_id}/out/solution.csv"

    matrix_content = _read_test_resource("matrix.csv")
    rhs_content = _read_test_resource("rhs.csv")

    _put_bytes(
        s3_client=s3_client,
        bucket=bucket,
        key=matrix_key,
        body=matrix_content,
    )
    _put_bytes(
        s3_client=s3_client,
        bucket=bucket,
        key=rhs_key,
        body=rhs_content,
    )

    storage = S3Storage(s3_client, bucket)
    preparer = InputArtifactPreparer(storage, tmp_path)

    prepared = preparer.prepare(
        user_id=user_id,
        job_id=job_id,
        inputs={
            "matrix": ArtifactRef(
                object_key=matrix_key,
                format=ArtifactFormat.CSV,
            ),
            "rhs": ArtifactRef(
                object_key=rhs_key,
                format=ArtifactFormat.CSV,
            ),
        },
    )

    manifest = cast(
        JobManifest,
        cast(
            object,
            SimpleNamespace(
                user_id=user_id,
                job_id=job_id,
                job_type="solving_slae",
                inputs={
                    "matrix": ArtifactRef(
                        object_key=matrix_key,
                        format=ArtifactFormat.CSV,
                    ),
                    "rhs": ArtifactRef(
                        object_key=rhs_key,
                        format=ArtifactFormat.CSV,
                    ),
                },
                params={
                    "solvingMethod": "numpy_exact_solver",
                },
                outputs={
                    "solution": ArtifactRef(
                        object_key=solution_key,
                        format=ArtifactFormat.CSV,
                    ),
                },
            ),
        ),
    )

    context = JobExecutionContextFactory(tmp_path).create(
        manifest=manifest,
        prepared_job_inputs=prepared,
    )

    expected_input_dir = tmp_path / str(user_id) / job_id / "in"
    expected_output_dir = tmp_path / str(user_id) / job_id / "out"
    expected_matrix_path = expected_input_dir / "matrix.csv"
    expected_rhs_path = expected_input_dir / "rhs.csv"
    expected_solution_path = expected_output_dir / "solution.csv"

    # This is the intended JobHandler-facing input API.
    assert context.inputs.path("matrix") == expected_matrix_path
    assert context.inputs.path("rhs") == expected_rhs_path
    assert context.inputs.read("matrix") == matrix_content
    assert context.inputs.read("rhs") == rhs_content

    # This is the intended JobHandler-facing parameter API.
    assert context.params.required("solvingMethod") == "numpy_exact_solver"

    # Compatibility API still delegates to the same artifact facade.
    assert context.input_path("matrix") == expected_matrix_path
    assert context.input_path("rhs") == expected_rhs_path
    assert context.required_param("solvingMethod") == "numpy_exact_solver"

    # This is the intended JobHandler-facing output API.
    solution_content = b"1.0\n2.0\n"
    context.outputs.write("solution", solution_content)

    assert context.outputs.path("solution") == expected_solution_path
    assert expected_solution_path.is_file()
    assert expected_solution_path.read_bytes() == solution_content

    # Runtime metadata is still available when needed.
    assert context.inputs.get("matrix").object_key == matrix_key
    assert context.inputs.get("matrix").format == ArtifactFormat.CSV
    assert context.outputs.get("solution").object_key == solution_key
    assert context.outputs.get("solution").format == ArtifactFormat.CSV

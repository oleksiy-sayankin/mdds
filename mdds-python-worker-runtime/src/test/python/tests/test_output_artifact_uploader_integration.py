# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from pathlib import Path

import boto3
import pytest
from botocore.config import Config
from testcontainers.minio import MinioContainer

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.domain.manifest import ArtifactRef
from mdds_worker_runtime.execution.artifacts import (
    InputArtifacts,
    JobParameters,
    OutputArtifacts,
    PreparedOutputArtifact,
)
from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.output_artifact_uploader import (
    OutputArtifactUploadResult,
    OutputArtifactUploader,
    UploadedOutputArtifact,
)
from mdds_worker_runtime.storage.s3_client import S3Storage
from mdds_worker_runtime.execution.object_keys import file_name_from_object_key

TEST_RESOURCES_DIR = Path(__file__).resolve().parents[2] / "resources"


def _read_test_resource(file_name: str) -> bytes:
    path = TEST_RESOURCES_DIR / file_name
    assert path.is_file(), f"Test resource does not exist: {path}"
    return path.read_bytes()


def _get_bytes(s3_client, bucket: str, key: str) -> bytes:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


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


def test_output_artifact_uploader_uploads_context_outputs_to_real_s3(
    s3_client_and_bucket,
    tmp_path: Path,
) -> None:
    s3_client, bucket = s3_client_and_bucket

    user_id = 12345
    job_id = "job-output-artifacts-upload"

    solution_1_key = f"jobs/{user_id}/{job_id}/out/solution_1.csv"
    solution_2_key = f"jobs/{user_id}/{job_id}/out/solution_2.csv"

    solution_1_content = _read_test_resource("solution_1.csv")
    solution_2_content = _read_test_resource("solution_2.csv")

    context = _create_context(
        tmp_path=tmp_path,
        user_id=user_id,
        job_id=job_id,
        outputs={
            "solution_1": ArtifactRef(
                object_key=solution_1_key,
                format=ArtifactFormat.CSV,
            ),
            "solution_2": ArtifactRef(
                object_key=solution_2_key,
                format=ArtifactFormat.CSV,
            ),
        },
    )

    context.outputs.write("solution_1", solution_1_content)
    context.outputs.write("solution_2", solution_2_content)

    expected_output_dir = tmp_path / str(user_id) / job_id / "out"
    expected_solution_1_path = expected_output_dir / "solution_1.csv"
    expected_solution_2_path = expected_output_dir / "solution_2.csv"

    assert expected_solution_1_path.is_file()
    assert expected_solution_2_path.is_file()

    storage = S3Storage(s3_client, bucket)
    uploader = OutputArtifactUploader(storage)

    result = uploader.upload(context)

    assert result == OutputArtifactUploadResult(
        user_id=user_id,
        job_id=job_id,
        job_type="solving_slae",
        uploaded_artifacts=(
            UploadedOutputArtifact(
                slot="solution_1",
                object_key=solution_1_key,
                local_path=expected_solution_1_path,
                size_bytes=len(solution_1_content),
            ),
            UploadedOutputArtifact(
                slot="solution_2",
                object_key=solution_2_key,
                local_path=expected_solution_2_path,
                size_bytes=len(solution_2_content),
            ),
        ),
    )

    assert _get_bytes(s3_client, bucket, solution_1_key) == solution_1_content
    assert _get_bytes(s3_client, bucket, solution_2_key) == solution_2_content


def _create_context(
    tmp_path: Path,
    user_id: int,
    job_id: str,
    outputs: dict[str, ArtifactRef],
) -> JobExecutionContext:
    work_dir = tmp_path / str(user_id) / job_id
    input_dir = work_dir / "in"
    output_dir = work_dir / "out"

    prepared_outputs = {
        output_slot: PreparedOutputArtifact(
            object_key=artifact_ref.object_key,
            local_path=output_dir / file_name_from_object_key(artifact_ref.object_key),
            format=artifact_ref.format,
        )
        for output_slot, artifact_ref in outputs.items()
    }

    return JobExecutionContext(
        user_id=user_id,
        job_id=job_id,
        job_type="solving_slae",
        work_dir=work_dir,
        input_dir=input_dir,
        output_dir=output_dir,
        inputs=InputArtifacts({}),
        outputs=OutputArtifacts(prepared_outputs),
        params=JobParameters({"solvingMethod": "numpy_exact_solver"}),
    )

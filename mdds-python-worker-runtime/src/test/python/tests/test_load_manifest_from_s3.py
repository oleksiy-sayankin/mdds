# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import json
from copy import deepcopy
from typing import Any
from unittest.mock import Mock

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from testcontainers.minio import MinioContainer

from mdds_worker_runtime.domain.artifact_format import (
    ArtifactFormat,
    UnknownArtifactFormatError,
)
from mdds_worker_runtime.manifest.errors import (
    ManifestDomainMappingError,
    ManifestLoadingError,
    ManifestSchemaValidationError,
)
from mdds_worker_runtime.manifest.loader import ManifestLoader
from mdds_worker_runtime.storage.s3_client import S3Storage


def _put_json(s3_client, bucket: str, key: str, value: object) -> None:
    # Converts manifest from Python dictionary to bytes
    body = json.dumps(value).encode("utf-8")

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )


def _put_bytes(
    s3_client,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str = "application/json",
) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )


def _new_loader(s3_client, bucket: str) -> ManifestLoader:
    storage = S3Storage(s3_client, bucket)
    return ManifestLoader(storage)


@pytest.fixture(scope="module")
def manifest_dict() -> dict[str, Any]:
    """
    This is sample manifest we want to load from S3.
    We created it in the form of Python dictionary.
    """
    return {
        "manifestVersion": 1,
        "userId": 12345,
        "jobId": "job-1",
        "jobType": "solving_slae",
        "inputs": {
            "matrix": {
                "objectKey": "jobs/12345/job-1/in/matrix.csv",
                "format": "csv",
            },
            "rhs": {
                "objectKey": "jobs/12345/job-1/in/rhs.csv",
                "format": "csv",
            },
        },
        "params": {
            "solvingMethod": "numpy_exact_solver",
        },
        "outputs": {
            "solution": {
                "objectKey": "jobs/12345/job-1/out/solution.csv",
                "format": "csv",
            },
        },
    }


@pytest.fixture(scope="module")
def s3_client_and_bucket():
    """Create minio client and bucket in test MinIO container"""
    bucket = "mdds-test"
    access_key = "minioadmin"
    secret_key = "minioadmin"

    # Get the MinIO container, use it and finally close.
    with MinioContainer(access_key=access_key, secret_key=secret_key) as minio:
        config = minio.get_config()

        endpoint = (
            config.get("endpoint")
            if isinstance(config, dict)
            else getattr(config, "endpoint")
        )

        # MinioContainer may return endpoint as <host>:<port>,
        # while boto3 requires a full URL with scheme.
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

        # Give s3_client and bucket to the test
        # and then return here to close the
        # MinIO container.
        yield s3_client, bucket


def test_load_dto_manifest_from_s3(s3_client_and_bucket, manifest_dict):
    s3_client, bucket = s3_client_and_bucket
    key = "jobs/12345/job-1/manifest.json"

    manifest = deepcopy(manifest_dict)
    _put_json(s3_client, bucket, key, manifest)

    storage = S3Storage(s3_client, bucket)
    loader = ManifestLoader(storage)

    actual = loader.load_dto(key)

    assert actual.manifest_version == 1
    assert actual.user_id == 12345
    assert actual.job_id == "job-1"
    assert actual.job_type == "solving_slae"

    assert actual.inputs["matrix"].object_key == "jobs/12345/job-1/in/matrix.csv"
    assert actual.inputs["matrix"].format == "csv"

    assert actual.inputs["rhs"].object_key == "jobs/12345/job-1/in/rhs.csv"
    assert actual.inputs["rhs"].format == "csv"

    assert actual.params["solvingMethod"] == "numpy_exact_solver"

    assert actual.outputs["solution"].object_key == "jobs/12345/job-1/out/solution.csv"
    assert actual.outputs["solution"].format == "csv"

    # Compare two dictionaries using aliases instead of field names.
    assert actual.model_dump(by_alias=True) == manifest_dict


def test_load_manifest_from_s3(s3_client_and_bucket, manifest_dict):
    s3_client, bucket = s3_client_and_bucket
    key = "jobs/12345/job-1/manifest.json"

    manifest = deepcopy(manifest_dict)
    _put_json(s3_client, bucket, key, manifest)

    storage = S3Storage(s3_client, bucket)
    loader = ManifestLoader(storage)

    actual = loader.load(key)

    assert actual.manifest_version == 1
    assert actual.user_id == 12345
    assert actual.job_id == "job-1"
    assert actual.job_type == "solving_slae"

    assert actual.inputs["matrix"].object_key == "jobs/12345/job-1/in/matrix.csv"
    assert actual.inputs["matrix"].format == ArtifactFormat.CSV

    assert actual.inputs["rhs"].object_key == "jobs/12345/job-1/in/rhs.csv"
    assert actual.inputs["rhs"].format == ArtifactFormat.CSV

    assert actual.params["solvingMethod"] == "numpy_exact_solver"

    assert actual.outputs["solution"].object_key == "jobs/12345/job-1/out/solution.csv"
    assert actual.outputs["solution"].format == ArtifactFormat.CSV


def test_load_manifest_from_s3_rejects_unsupported_artifact_format(
    s3_client_and_bucket,
    manifest_dict,
):
    s3_client, bucket = s3_client_and_bucket
    key = "jobs/12345/job-unsupported-format/manifest.json"

    manifest = deepcopy(manifest_dict)
    manifest["jobId"] = "job-unsupported-format"
    manifest["inputs"]["matrix"]["format"] = "wrong_format"

    _put_json(s3_client, bucket, key, manifest)

    loader = _new_loader(s3_client, bucket)

    with pytest.raises(ManifestDomainMappingError) as error:
        loader.load(key)

    assert error.value.key == key
    assert (
        str(error.value)
        == f"Manifest object '{key}' cannot be converted to domain model: "
        "Unknown or unsupported artifact format: 'wrong_format'."
    )
    assert isinstance(error.value.__cause__, UnknownArtifactFormatError)


def test_load_dto_manifest_from_s3_rejects_missing_required_field(
    s3_client_and_bucket,
    manifest_dict,
):
    s3_client, bucket = s3_client_and_bucket
    key = "jobs/12345/job-missing-job-id/manifest.json"

    manifest = deepcopy(manifest_dict)
    manifest.pop("jobId")

    _put_json(s3_client, bucket, key, manifest)

    loader = _new_loader(s3_client, bucket)

    with pytest.raises(ManifestSchemaValidationError) as error:
        loader.load_dto(key)

    assert error.value.key == key
    assert (
        str(error.value) == f"Manifest object '{key}' does not match manifest schema."
    )


def test_load_dto_manifest_from_s3_rejects_unsupported_manifest_version(
    s3_client_and_bucket,
    manifest_dict,
):
    s3_client, bucket = s3_client_and_bucket
    key = "jobs/12345/job-bad-version/manifest.json"

    manifest = deepcopy(manifest_dict)
    manifest["jobId"] = "job-bad-version"
    manifest["manifestVersion"] = 12345

    _put_json(s3_client, bucket, key, manifest)

    loader = _new_loader(s3_client, bucket)

    with pytest.raises(ManifestSchemaValidationError) as error:
        loader.load_dto(key)

    assert error.value.key == key
    assert (
        str(error.value) == f"Manifest object '{key}' does not match manifest schema."
    )
    assert "manifestVersion" in str(error.value.__cause__)


def test_load_dto_manifest_from_s3_rejects_extra_field(
    s3_client_and_bucket,
    manifest_dict,
):
    s3_client, bucket = s3_client_and_bucket
    key = "jobs/12345/job-extra-field/manifest.json"

    manifest = deepcopy(manifest_dict)
    manifest["jobId"] = "job-extra-field"
    manifest["unexpected"] = "field"

    _put_json(s3_client, bucket, key, manifest)

    loader = _new_loader(s3_client, bucket)

    with pytest.raises(ManifestSchemaValidationError) as error:
        loader.load_dto(key)

    assert error.value.key == key
    assert "unexpected" in str(error.value.__cause__)


def test_load_dto_manifest_from_s3_rejects_extra_nested_artifact_field(
    s3_client_and_bucket,
    manifest_dict,
):
    s3_client, bucket = s3_client_and_bucket
    key = "jobs/12345/job-extra-artifact-field/manifest.json"

    manifest = deepcopy(manifest_dict)
    manifest["jobId"] = "job-extra-artifact-field"
    manifest["inputs"]["matrix"]["unexpected"] = "field"

    _put_json(s3_client, bucket, key, manifest)

    loader = _new_loader(s3_client, bucket)

    with pytest.raises(ManifestSchemaValidationError) as error:
        loader.load_dto(key)

    assert error.value.key == key
    assert "unexpected" in str(error.value.__cause__)


def test_load_dto_manifest_from_s3_missing_object_key(s3_client_and_bucket):
    s3_client, bucket = s3_client_and_bucket
    key = "jobs/12345/missing-job/manifest.json"

    loader = _new_loader(s3_client, bucket)

    with pytest.raises(ManifestLoadingError) as error:
        loader.load_dto(key)

    assert error.value.key == key
    assert str(error.value) == f"Key '{key}' not found in storage."
    assert error.value.error_code in {"NoSuchKey", "404", "NotFound"}


def test_load_dto_manifest_from_s3_rejects_invalid_json(s3_client_and_bucket):
    s3_client, bucket = s3_client_and_bucket
    key = "jobs/12345/job-invalid-json/manifest.json"

    _put_bytes(
        s3_client=s3_client,
        bucket=bucket,
        key=key,
        body=b"{ invalid json",
    )

    loader = _new_loader(s3_client, bucket)

    with pytest.raises(ManifestLoadingError) as error:
        loader.load_dto(key)

    assert error.value.key == key
    assert str(error.value) == f"Manifest object '{key}' contains invalid JSON."
    assert isinstance(error.value.__cause__, json.JSONDecodeError)


def test_load_dto_manifest_from_s3_rejects_non_object_json_root(
    s3_client_and_bucket,
):
    s3_client, bucket = s3_client_and_bucket
    key = "jobs/12345/job-json-array/manifest.json"

    _put_json(s3_client, bucket, key, ["not", "a", "manifest"])

    loader = _new_loader(s3_client, bucket)

    with pytest.raises(ManifestSchemaValidationError) as error:
        loader.load_dto(key)

    assert error.value.key == key


def test_load_dto_wraps_non_not_found_client_error():
    key = "jobs/12345/job-1/manifest.json"

    storage = Mock()
    storage.get_json.side_effect = ClientError(
        error_response={
            "Error": {
                "Code": "AccessDenied",
                "Message": "Access denied",
            }
        },
        operation_name="GetObject",
    )

    loader = ManifestLoader(storage)

    with pytest.raises(ManifestLoadingError) as error:
        loader.load_dto(key)

    assert error.value.key == key
    assert "AccessDenied" in str(error.value)
    assert error.value.error_code == "AccessDenied"


def test_load_dto_manifest_from_s3_rejects_non_utf8_json(s3_client_and_bucket):
    s3_client, bucket = s3_client_and_bucket
    key = "jobs/12345/job-non-utf8-json/manifest.json"

    _put_bytes(
        s3_client=s3_client,
        bucket=bucket,
        key=key,
        body=b"\xff\xfe\x00\x00",
    )

    loader = _new_loader(s3_client, bucket)

    with pytest.raises(ManifestLoadingError) as error:
        loader.load_dto(key)

    assert error.value.key == key
    assert str(error.value) == f"Manifest object '{key}' contains invalid JSON."
    assert isinstance(error.value.__cause__, UnicodeDecodeError)

# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import pytest
from pydantic import ValidationError

from mdds_worker_runtime.domain.artifact_format import (
    ArtifactFormat,
    UnknownArtifactFormatError,
)
from mdds_worker_runtime.manifest.models import JobManifestDTO
from mdds_worker_runtime.manifest import mapper


def test_to_domain():
    job_manifest_dto = JobManifestDTO(
        manifestVersion=1,
        userId=12345,
        jobId="job-1",
        jobType="solving_slae",
        inputs={
            "matrix": {
                "objectKey": "jobs/12345/job-1/in/matrix.csv",
                "format": "csv",
            },
            "rhs": {
                "objectKey": "jobs/12345/job-1/in/rhs.csv",
                "format": "csv",
            },
        },
        params={
            "solvingMethod": "numpy_exact_solver",
        },
        outputs={
            "solution": {
                "objectKey": "jobs/12345/job-1/out/solution.csv",
                "format": "csv",
            },
        },
    )

    actual = mapper.to_domain(job_manifest_dto)
    assert actual.manifest_version == 1
    assert actual.user_id == 12345
    assert actual.job_id == "job-1"
    assert actual.job_type == "solving_slae"

    assert actual.inputs["matrix"].slot_name == "matrix"
    assert actual.inputs["rhs"].slot_name == "rhs"
    assert actual.outputs["solution"].slot_name == "solution"

    assert actual.inputs["matrix"].object_key == "jobs/12345/job-1/in/matrix.csv"
    assert actual.inputs["matrix"].format == ArtifactFormat.CSV

    assert actual.inputs["rhs"].object_key == "jobs/12345/job-1/in/rhs.csv"
    assert actual.inputs["rhs"].format == ArtifactFormat.CSV

    assert actual.params["solvingMethod"] == "numpy_exact_solver"

    assert actual.outputs["solution"].object_key == "jobs/12345/job-1/out/solution.csv"
    assert actual.outputs["solution"].format == ArtifactFormat.CSV


def test_to_domain_unknown_artifact_format():
    dto = JobManifestDTO(
        manifestVersion=1,
        userId=12345,
        jobId="job-1",
        jobType="solving_slae",
        inputs={
            "matrix": {
                "objectKey": "jobs/12345/job-1/in/matrix.csv",
                "format": "wrong_format",
            }
        },
        params={},
        outputs={},
    )

    with pytest.raises(UnknownArtifactFormatError) as error:
        mapper.to_domain(dto)

    assert str(error.value) == "Unknown or unsupported artifact format: 'wrong_format'."


def test_manifest_dto_default_empty_collections():
    dto = JobManifestDTO(
        manifestVersion=1,
        userId=12345,
        jobId="job-1",
        jobType="solving_slae",
    )

    assert dto.inputs == {}
    assert dto.params == {}
    assert dto.outputs == {}


def test_to_domain_preserves_json_params():
    dto = JobManifestDTO(
        manifestVersion=1,
        userId=12345,
        jobId="job-1",
        jobType="solving_slae",
        params={
            "solvingMethod": "numpy_exact_solver",
            "tolerance": 1e-9,
            "enabled": True,
            "options": {"ksp": "gmres"},  # This JSON value we want to preserve
            "items": [1, 2, 3],
            "nullable": None,
        },
    )

    actual = mapper.to_domain(dto)

    assert actual.params == dto.params
    assert actual.params is not dto.params


def test_manifest_dto_accepts_python_field_names():
    dto = JobManifestDTO(
        manifest_version=1,
        user_id=12345,
        job_id="job-1",
        job_type="solving_slae",
    )

    assert dto.manifest_version == 1
    assert dto.user_id == 12345
    assert dto.job_id == "job-1"
    assert dto.job_type == "solving_slae"


def test_manifest_dto_rejects_unsupported_manifest_version():
    with pytest.raises(ValidationError):
        JobManifestDTO(
            manifestVersion=2,
            userId=12345,
            jobId="job-1",
            jobType="solving_slae",
        )


def test_manifest_dto_rejects_blank_job_id():
    with pytest.raises(ValidationError):
        JobManifestDTO(
            manifestVersion=1,
            userId=12345,
            jobId=" ",
            jobType="solving_slae",
        )


def test_manifest_dto_rejects_extra_fields():
    with pytest.raises(ValidationError):
        JobManifestDTO.model_validate(
            {
                "manifestVersion": 1,
                "userId": 12345,
                "jobId": "job-1",
                "jobType": "solving_slae",
                "unexpected": "field",
            }
        )


def test_manifest_dto_rejects_blank_artifact_object_key():
    with pytest.raises(ValidationError):
        JobManifestDTO(
            manifestVersion=1,
            userId=12345,
            jobId="job-1",
            jobType="solving_slae",
            inputs={
                "matrix": {
                    "objectKey": " ",
                    "format": "csv",
                }
            },
        )


def test_manifest_dto_rejects_blank_artifact_format():
    with pytest.raises(ValidationError):
        JobManifestDTO(
            manifestVersion=1,
            userId=12345,
            jobId="job-1",
            jobType="solving_slae",
            inputs={
                "matrix": {
                    "objectKey": "jobs/12345/job-1/in/matrix.csv",
                    "format": " ",
                }
            },
        )


@pytest.mark.parametrize("user_id", [0, -1])
def test_manifest_dto_rejects_non_positive_user_id(user_id):
    with pytest.raises(ValidationError):
        JobManifestDTO(
            manifestVersion=1,
            userId=user_id,
            jobId="job-1",
            jobType="solving_slae",
        )

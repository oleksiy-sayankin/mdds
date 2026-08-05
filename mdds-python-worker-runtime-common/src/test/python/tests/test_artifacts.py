# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from pathlib import Path

import pytest
from mdds_worker_runtime_common.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime_common.execution.artifacts import (
    InputArtifacts,
    OutputArtifacts,
    PreparedInputArtifact,
    PreparedOutputArtifact,
    WorkerParameters,
)


def test_input_artifacts_use_snapshot_of_source_mapping(tmp_path: Path) -> None:
    artifact = PreparedInputArtifact(
        local_path=tmp_path / "input.csv",
        format=ArtifactFormat.CSV,
    )
    source = {"matrix": artifact}

    artifacts = InputArtifacts(source)
    source.clear()

    assert artifacts.get("matrix") == artifact
    assert artifacts.path("matrix") == artifact.local_path
    assert dict(artifacts.items()) == {"matrix": artifact}


def test_input_artifacts_reject_null_mapping() -> None:
    with pytest.raises(ValueError, match="input artifacts cannot be null"):
        InputArtifacts(None)  # type: ignore[arg-type]


@pytest.mark.parametrize("slot", [None, "", " \t\n"])
def test_input_artifacts_reject_null_or_blank_slot(slot: str | None) -> None:
    artifacts = InputArtifacts({})

    with pytest.raises(ValueError) as exc_info:
        artifacts.get(slot)  # type: ignore[arg-type]

    assert str(exc_info.value) == "input slot cannot be null or blank."


def test_input_artifacts_report_missing_slot() -> None:
    artifacts = InputArtifacts({})

    with pytest.raises(KeyError) as exc_info:
        artifacts.get("rhs")

    assert exc_info.value.args == ("Input slot is not available: rhs",)
    assert isinstance(exc_info.value.__cause__, KeyError)


def test_input_artifacts_read_bytes(tmp_path: Path) -> None:
    input_path = tmp_path / "input.csv"
    input_path.write_bytes(b"1,2\n3,4\n")
    artifacts = InputArtifacts(
        {
            "payload": PreparedInputArtifact(
                local_path=input_path,
                format=ArtifactFormat.CSV,
            ),
        }
    )

    assert artifacts.read("payload") == b"1,2\n3,4\n"


def test_output_artifacts_use_snapshot_of_source_mapping(tmp_path: Path) -> None:
    artifact = PreparedOutputArtifact(
        local_path=tmp_path / "output.csv",
        format=ArtifactFormat.CSV,
    )
    source = {"solution": artifact}

    artifacts = OutputArtifacts(source)
    source.clear()

    assert artifacts.get("solution") == artifact
    assert artifacts.path("solution") == artifact.local_path
    assert dict(artifacts.items()) == {"solution": artifact}


def test_output_artifacts_reject_null_mapping() -> None:
    with pytest.raises(ValueError, match="output artifacts cannot be null"):
        OutputArtifacts(None)  # type: ignore[arg-type]


def test_output_artifacts_reject_blank_slot() -> None:
    artifacts = OutputArtifacts({})

    with pytest.raises(ValueError) as exc_info:
        artifacts.get("  ")

    assert str(exc_info.value) == "output slot cannot be null or blank."


def test_output_artifacts_report_undeclared_slot() -> None:
    artifacts = OutputArtifacts({})

    with pytest.raises(KeyError) as exc_info:
        artifacts.get("solution")

    assert exc_info.value.args == ("Output slot is not declared: solution",)
    assert isinstance(exc_info.value.__cause__, KeyError)


def test_output_artifacts_write_bytes_and_create_parent_directories(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "nested" / "outputs" / "solution.csv"
    artifacts = OutputArtifacts(
        {
            "solution": PreparedOutputArtifact(
                local_path=output_path,
                format=ArtifactFormat.CSV,
            ),
        }
    )

    artifacts.write("solution", b"1.0,2.0\n")

    assert output_path.read_bytes() == b"1.0,2.0\n"


def test_output_artifacts_reject_null_data(tmp_path: Path) -> None:
    output_path = tmp_path / "output.csv"
    artifacts = OutputArtifacts(
        {
            "result": PreparedOutputArtifact(
                local_path=output_path,
                format=ArtifactFormat.CSV,
            ),
        }
    )

    with pytest.raises(ValueError, match="output data cannot be null"):
        artifacts.write("result", None)  # type: ignore[arg-type]

    assert not output_path.exists()


def test_output_artifacts_reject_non_bytes_data(tmp_path: Path) -> None:
    output_path = tmp_path / "output.csv"
    artifacts = OutputArtifacts(
        {
            "result": PreparedOutputArtifact(
                local_path=output_path,
                format=ArtifactFormat.CSV,
            ),
        }
    )

    with pytest.raises(TypeError, match="output data must be bytes"):
        artifacts.write("result", bytearray(b"data"))  # type: ignore[arg-type]

    assert not output_path.exists()


def test_worker_parameters_use_snapshot_of_source_mapping() -> None:
    source = {"method": "numpy_exact_solver", "tolerance": 1e-8}

    params = WorkerParameters(source)
    source["method"] = "changed"

    assert params.get("method") == "numpy_exact_solver"
    assert params.get("missing") is None
    assert params.get("missing", "fallback") == "fallback"
    assert params.required("tolerance") == 1e-8
    assert dict(params.items()) == {
        "method": "numpy_exact_solver",
        "tolerance": 1e-8,
    }


def test_worker_parameters_reject_null_mapping() -> None:
    with pytest.raises(ValueError, match="worker parameters cannot be null"):
        WorkerParameters(None)  # type: ignore[arg-type]


def test_worker_parameters_reject_blank_name() -> None:
    params = WorkerParameters({})

    with pytest.raises(ValueError) as exc_info:
        params.required("\t")

    assert str(exc_info.value) == "parameter name cannot be null or blank."


def test_worker_parameters_report_missing_required_parameter() -> None:
    params = WorkerParameters({})

    with pytest.raises(KeyError) as exc_info:
        params.required("tolerance")

    assert exc_info.value.args == ("Required parameter is missing: tolerance",)

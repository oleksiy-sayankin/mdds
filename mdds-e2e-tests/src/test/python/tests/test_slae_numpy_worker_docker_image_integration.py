# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""Integration test for the Python NumPy SLAE Worker Docker image."""

from __future__ import annotations

import base64
import csv
import json
from collections.abc import Sequence
from io import StringIO

import pytest
from testcontainers.core.container import DockerContainer

_SLAE_NUMPY_WORKER_IMAGE = "mddsproject/python-worker-solving-slae-numpy:0.1.0"

_WORKER_BINARY = "/opt/mdds/bin/mdds-worker"
_WORKER_MANIFEST_PATH = "/opt/mdds/config/worker-manifest.json"
_WORKER_RESULT_PATH = "/opt/mdds/result/result.json"

_MATRIX_INPUT_SLOT = "matrix"
_RHS_INPUT_SLOT = "rhs"
_SOLUTION_OUTPUT_SLOT = "solution"

_MATRIX_PATH = f"/opt/mdds/inputs/{_MATRIX_INPUT_SLOT}"
_RHS_PATH = f"/opt/mdds/inputs/{_RHS_INPUT_SLOT}"
_SOLUTION_PATH = f"/opt/mdds/outputs/{_SOLUTION_OUTPUT_SLOT}"

_MATRIX = (
    (2.0, 1.0, -1.0),
    (-3.0, -1.0, 2.0),
    (-2.0, 1.0, 2.0),
)
_RHS = (8.0, -11.0, -3.0)
_EXPECTED_SOLUTION = (2.0, 3.0, -1.0)


def test_slae_numpy_worker_docker_image_solves_linear_system() -> None:
    """Run the MDDS contract executable and verify output and result files."""
    with _new_slae_numpy_worker_container() as worker_container:
        _write_worker_manifest(worker_container)
        _write_file_in_container(
            worker_container,
            _MATRIX_PATH,
            _csv_matrix_bytes(_MATRIX),
        )
        _write_file_in_container(
            worker_container,
            _RHS_PATH,
            _csv_vector_bytes(_RHS),
        )

        worker_exit_code, worker_output = _exec_in_container(
            worker_container,
            [_WORKER_BINARY],
        )

        assert worker_exit_code == 0, (
            "NumPy SLAE Worker failed.\n"
            f"Container output:\n{_decode_output(worker_output)}"
        )

        solution_bytes = _read_required_file(
            worker_container,
            _SOLUTION_PATH,
        )
        actual_solution = _read_csv_vector(solution_bytes)
        assert actual_solution == pytest.approx(_EXPECTED_SOLUTION)

        result_bytes = _read_required_file(
            worker_container,
            _WORKER_RESULT_PATH,
        )
        assert json.loads(result_bytes.decode("utf-8")) == {
            "exitCode": 0,
            "message": "WorkerHandler execution completed.",
        }


def _new_slae_numpy_worker_container() -> DockerContainer:
    """Start the image in an idle state so the test can prepare one attempt."""
    # The Worker Image owns MDDS_WORKER_NAME, MDDS_WORKER_VERSION, and
    # MDDS_WORKER_HANDLER. Argo supplies only the retry index per attempt.
    return (
        DockerContainer(_SLAE_NUMPY_WORKER_IMAGE)
        .with_env("MDDS_ARGO_RETRY_INDEX", "0")
        .with_command(["sleep", "infinity"])
    )


def _write_worker_manifest(worker_container: DockerContainer) -> None:
    manifest = {
        "apiVersion": "mdds/v1",
        "kind": "WorkerManifest",
        "execution": {
            "userId": 12345,
            "dagRunId": "slae-numpy-integration-test",
            "nodeId": "solve-slae-with-numpy",
        },
        "inputs": {
            _MATRIX_INPUT_SLOT: {
                "path": _MATRIX_PATH,
                "format": "csv",
            },
            _RHS_INPUT_SLOT: {
                "path": _RHS_PATH,
                "format": "csv",
            },
        },
        "params": {},
        "outputs": {
            _SOLUTION_OUTPUT_SLOT: {
                "path": _SOLUTION_PATH,
                "format": "csv",
            },
        },
    }
    manifest_bytes = (
        json.dumps(manifest, ensure_ascii=False, allow_nan=False, indent=2) + "\n"
    ).encode("utf-8")

    _write_file_in_container(
        worker_container,
        _WORKER_MANIFEST_PATH,
        manifest_bytes,
    )


def _write_file_in_container(
    worker_container: DockerContainer,
    path: str,
    content: bytes,
) -> None:
    """Materialize an Argo-provided file as root inside the container."""
    encoded_content = base64.b64encode(content).decode("ascii")
    script = (
        "import base64\n"
        "from pathlib import Path\n"
        f"path = Path({path!r})\n"
        "path.parent.mkdir(parents=True, exist_ok=True)\n"
        f"path.write_bytes(base64.b64decode({encoded_content!r}))\n"
    )
    exit_code, output = _exec_in_container(
        worker_container,
        ["python", "-c", script],
        user="root",
    )

    assert exit_code == 0, (
        f"Cannot create file inside container: {path}.\n"
        f"Container output:\n{_decode_output(output)}"
    )


def _read_required_file(
    worker_container: DockerContainer,
    path: str,
) -> bytes:
    file_exit_code, file_check_output = _exec_in_container(
        worker_container,
        ["test", "-f", path],
    )
    assert file_exit_code == 0, (
        f"Expected file does not exist: {path}.\n"
        f"Container output:\n{_decode_output(file_check_output)}"
    )

    read_exit_code, content = _exec_in_container(
        worker_container,
        ["cat", path],
    )
    assert read_exit_code == 0, (
        f"Cannot read file: {path}.\n" f"Container output:\n{_decode_output(content)}"
    )
    return content


def _exec_in_container(
    worker_container: DockerContainer,
    command: list[str],
    *,
    user: str | None = None,
) -> tuple[int, bytes]:
    container = worker_container.get_wrapped_container()
    if user is None:
        result = container.exec_run(command)
    else:
        result = container.exec_run(command, user=user)

    if not isinstance(result.output, bytes):
        raise AssertionError(
            "Expected combined container command output to be bytes, "
            f"got {type(result.output).__name__}."
        )

    return int(result.exit_code), result.output


def _csv_matrix_bytes(matrix: Sequence[Sequence[float]]) -> bytes:
    content = StringIO(newline="")
    writer = csv.writer(content, lineterminator="\n")
    writer.writerows(matrix)
    return content.getvalue().encode("utf-8")


def _csv_vector_bytes(vector: Sequence[float]) -> bytes:
    content = StringIO(newline="")
    writer = csv.writer(content, lineterminator="\n")
    writer.writerows((value,) for value in vector)
    return content.getvalue().encode("utf-8")


def _read_csv_vector(content: bytes) -> list[float]:
    values: list[float] = []

    for row in csv.reader(StringIO(content.decode("utf-8"))):
        if not row or all(cell.strip() == "" for cell in row):
            continue

        if len(row) != 1:
            raise AssertionError(f"Expected one value per solution row, got: {row}")

        values.append(float(row[0]))

    return values


def _decode_output(output: bytes) -> str:
    return output.decode("utf-8", errors="replace")

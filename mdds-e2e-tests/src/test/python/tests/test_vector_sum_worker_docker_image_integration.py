# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""Integration test for the Python Vector Sum Worker Docker image."""

from __future__ import annotations

import base64
import csv
import json
from collections.abc import Sequence
from io import StringIO

import pytest
from testcontainers.core.container import DockerContainer

_VECTOR_SUM_WORKER_IMAGE = "mddsproject/python-worker-vector-sum:0.1.0"

_WORKER_BINARY = "/opt/mdds/bin/mdds-worker"
_WORKER_MANIFEST_PATH = "/opt/mdds/config/worker-manifest.json"

_VECTOR_A_INPUT_SLOT = "vector-a"
_VECTOR_B_INPUT_SLOT = "vector-b"
_SOLUTION_OUTPUT_SLOT = "solution"

_VECTOR_A_PATH = f"/opt/mdds/inputs/{_VECTOR_A_INPUT_SLOT}"
_VECTOR_B_PATH = f"/opt/mdds/inputs/{_VECTOR_B_INPUT_SLOT}"
_SOLUTION_PATH = f"/opt/mdds/outputs/{_SOLUTION_OUTPUT_SLOT}"

_VECTOR_A = [1.5, -2.0, 3.25]
_VECTOR_B = [2.5, 5.0, -1.25]
_EXPECTED_SOLUTION = [4.0, 3.0, 2.0]


def test_vector_sum_worker_docker_image_runs_as_non_root_mdds_user() -> None:
    """Verify the default user configured by the final Worker Image."""
    with _new_vector_sum_worker_container() as worker_container:
        name_exit_code, name_output = _exec_in_container(
            worker_container,
            ["id", "-un"],
        )
        uid_exit_code, uid_output = _exec_in_container(
            worker_container,
            ["id", "-u"],
        )

        assert name_exit_code == 0, (
            "Cannot determine the default container user.\n"
            f"Container output:\n{_decode_output(name_output)}"
        )
        assert uid_exit_code == 0, (
            "Cannot determine the default container user ID.\n"
            f"Container output:\n{_decode_output(uid_output)}"
        )
        assert _decode_output(name_output).strip() == "mdds"
        assert int(_decode_output(uid_output).strip()) != 0


def test_vector_sum_worker_docker_image_sums_two_csv_vectors() -> None:
    """Run the MDDS contract executable and verify its output artifact."""
    with _new_vector_sum_worker_container() as worker_container:
        _write_worker_manifest(worker_container)
        _write_file_in_container(
            worker_container,
            _VECTOR_A_PATH,
            _csv_vector_bytes(_VECTOR_A),
        )
        _write_file_in_container(
            worker_container,
            _VECTOR_B_PATH,
            _csv_vector_bytes(_VECTOR_B),
        )

        worker_exit_code, worker_output = _exec_in_container(
            worker_container,
            [_WORKER_BINARY],
        )

        assert worker_exit_code == 0, (
            "Vector Sum Worker failed.\n"
            f"Container output:\n{_decode_output(worker_output)}"
        )

        file_exit_code, file_check_output = _exec_in_container(
            worker_container,
            ["test", "-f", _SOLUTION_PATH],
        )
        assert file_exit_code == 0, (
            f"Expected output file does not exist: {_SOLUTION_PATH}.\n"
            f"Container output:\n{_decode_output(file_check_output)}"
        )

        read_exit_code, solution_bytes = _exec_in_container(
            worker_container,
            ["cat", _SOLUTION_PATH],
        )
        assert read_exit_code == 0, (
            f"Cannot read output file: {_SOLUTION_PATH}.\n"
            f"Container output:\n{_decode_output(solution_bytes)}"
        )

        actual_solution = _read_csv_vector(solution_bytes)

        assert actual_solution == pytest.approx(_EXPECTED_SOLUTION)


def _new_vector_sum_worker_container() -> DockerContainer:
    """Start the image in an idle state so the test can prepare one attempt."""
    # The Worker Image owns MDDS_WORKER_NAME, MDDS_WORKER_VERSION, and
    # MDDS_WORKER_HANDLER. Argo supplies only the retry index per attempt.
    return (
        DockerContainer(_VECTOR_SUM_WORKER_IMAGE)
        .with_env("MDDS_ARGO_RETRY_INDEX", "0")
        .with_command(["sleep", "infinity"])
    )


def _write_worker_manifest(worker_container: DockerContainer) -> None:
    manifest = {
        "apiVersion": "mdds/v1",
        "kind": "WorkerManifest",
        "execution": {
            "userId": 12345,
            "dagRunId": "vector-sum-integration-test",
            "nodeId": "sum-vectors",
        },
        "inputs": {
            _VECTOR_A_INPUT_SLOT: {
                "path": _VECTOR_A_PATH,
                "format": "csv",
            },
            _VECTOR_B_INPUT_SLOT: {
                "path": _VECTOR_B_PATH,
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
    """Materialize an Argo-provided file as root inside the running container."""
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

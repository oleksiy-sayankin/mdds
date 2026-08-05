# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from pathlib import Path

import pytest

from mdds_worker_runtime_common.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime_common.domain.manifest import WorkerManifest
from mdds_worker_runtime_common.manifest.loader import (
    WorkerManifestLoadError,
    load_worker_manifest,
)
from mdds_worker_runtime_common.manifest.mapper import (
    WorkerManifestMappingError,
    to_domain,
)

MANIFEST_RESOURCES = (
    Path(__file__).resolve().parents[2] / "resources" / "worker-manifests"
)


def _manifest_path(filename: str) -> Path:
    return MANIFEST_RESOURCES / filename


def test_loads_valid_manifest_as_domain_model() -> None:
    manifest = load_worker_manifest(_manifest_path("valid-worker-manifest.json"))

    assert isinstance(manifest, WorkerManifest)
    assert manifest.api_version == "mdds/v1"
    assert manifest.kind == "WorkerManifest"
    assert manifest.execution.user_id == 12345
    assert manifest.execution.dag_run_id == "dag-run-1"
    assert manifest.execution.node_id == "solve-a"
    assert manifest.inputs["matrix"].path == Path("/opt/mdds/inputs/matrix")
    assert manifest.inputs["matrix"].format is ArtifactFormat.CSV
    assert manifest.params["tolerance"] == 1e-8


def test_rejects_malformed_json() -> None:
    with pytest.raises(
        WorkerManifestLoadError,
        match="Invalid Worker Manifest",
    ):
        load_worker_manifest(_manifest_path("malformed-worker-manifest.json"))


def test_rejects_unknown_json_field() -> None:
    with pytest.raises(
        WorkerManifestLoadError,
        match="unexpected: Extra inputs are not permitted",
    ):
        load_worker_manifest(_manifest_path("unknown-field-worker-manifest.json"))


def test_rejects_wrong_api_version() -> None:
    with pytest.raises(WorkerManifestLoadError, match="apiVersion"):
        load_worker_manifest(_manifest_path("wrong-api-version-worker-manifest.json"))


def test_rejects_non_integer_user_id_instead_of_coercing_it() -> None:
    with pytest.raises(
        WorkerManifestLoadError,
        match=r"execution\.userId",
    ):
        load_worker_manifest(_manifest_path("non-integer-user-id-worker-manifest.json"))


def test_rejects_unknown_artifact_format_during_mapping() -> None:
    with pytest.raises(
        WorkerManifestLoadError,
        match="Unknown or unsupported artifact format",
    ):
        load_worker_manifest(
            _manifest_path("unknown-artifact-format-worker-manifest.json")
        )


def test_rejects_relative_artifact_path_during_mapping() -> None:
    with pytest.raises(
        WorkerManifestLoadError,
        match="Artifact path must be absolute",
    ):
        load_worker_manifest(
            _manifest_path("relative-artifact-path-worker-manifest.json")
        )


def test_reports_missing_manifest() -> None:
    with pytest.raises(
        WorkerManifestLoadError,
        match="Cannot read Worker Manifest",
    ):
        load_worker_manifest(_manifest_path("missing-worker-manifest.json"))


def test_mapper_rejects_null_dto() -> None:
    with pytest.raises(
        WorkerManifestMappingError,
        match="must not be null",
    ):
        to_domain(None)  # type: ignore[arg-type]

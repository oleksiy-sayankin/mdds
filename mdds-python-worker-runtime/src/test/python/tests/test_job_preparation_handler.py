# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.domain.artifact_format import ArtifactFormat
from mdds_worker_runtime.domain.manifest import ArtifactRef, JobManifest
from mdds_worker_runtime.execution.job_preparation_handler import (
    JobPreparationHandler,
    PreparedJob,
)
from mdds_worker_runtime.execution.workspace import JobWorkspace


def test_job_preparation_handler_returns_prepared_job_when_preparation_succeeds() -> (
    None
):
    fixture = _fixture()
    workspace = _workspace()

    result = fixture.handler.prepare(
        workspace=workspace,
    )

    assert isinstance(result, PreparedJob)
    assert result.context is fixture.context
    assert result.handler is fixture.job_handler

    fixture.input_artifact_preparer.prepare.assert_called_once_with(workspace)

    fixture.context_factory.create.assert_called_once_with(
        workspace=workspace,
        prepared_job_inputs=fixture.prepared_job_inputs,
    )
    fixture.input_artifact_preparer.prepare.assert_called_once_with(workspace)
    fixture.job_handler_loader.load.assert_called_once_with()


def test_job_preparation_handler_propagates_input_preparation_failure() -> None:
    fixture = _fixture()
    workspace = _workspace()

    fixture.input_artifact_preparer.prepare.side_effect = RuntimeError(
        "Cannot download input artifact."
    )

    with pytest.raises(RuntimeError, match="Cannot download input artifact."):
        fixture.handler.prepare(
            workspace=workspace,
        )

    fixture.input_artifact_preparer.prepare.assert_called_once_with(workspace)

    fixture.context_factory.create.assert_not_called()
    fixture.execution_registry.attach_context.assert_not_called()
    fixture.job_handler_loader.load.assert_not_called()


def test_job_preparation_handler_propagates_context_creation_failure() -> None:
    fixture = _fixture()
    workspace = _workspace()

    fixture.context_factory.create.side_effect = RuntimeError(
        "Cannot save context snapshot."
    )

    with pytest.raises(RuntimeError, match="Cannot save context snapshot."):
        fixture.handler.prepare(
            workspace=workspace,
        )

    fixture.input_artifact_preparer.prepare.assert_called_once_with(workspace)
    fixture.context_factory.create.assert_called_once_with(
        workspace=workspace,
        prepared_job_inputs=fixture.prepared_job_inputs,
    )

    fixture.execution_registry.attach_context.assert_not_called()
    fixture.job_handler_loader.load.assert_not_called()


def test_job_preparation_handler_propagates_context_attach_failure() -> None:
    fixture = _fixture()
    workspace = _workspace()

    fixture.execution_registry.attach_context.side_effect = RuntimeError(
        "Cannot attach context."
    )

    with pytest.raises(RuntimeError, match="Cannot attach context."):
        fixture.handler.prepare(
            workspace=workspace,
        )

    fixture.input_artifact_preparer.prepare.assert_called_once_with(workspace)
    fixture.context_factory.create.assert_called_once_with(
        workspace=workspace,
        prepared_job_inputs=fixture.prepared_job_inputs,
    )
    fixture.execution_registry.attach_context.assert_called_once_with(
        job_id="job-1",
        context=fixture.context,
    )

    fixture.job_handler_loader.load.assert_not_called()


def test_job_preparation_handler_propagates_handler_loading_failure() -> None:
    fixture = _fixture()
    workspace = _workspace()

    fixture.job_handler_loader.load.side_effect = RuntimeError(
        "Cannot load job handler."
    )

    with pytest.raises(RuntimeError, match="Cannot load job handler."):
        fixture.handler.prepare(
            workspace=workspace,
        )

    fixture.input_artifact_preparer.prepare.assert_called_once_with(workspace)
    fixture.context_factory.create.assert_called_once_with(
        workspace=workspace,
        prepared_job_inputs=fixture.prepared_job_inputs,
    )
    fixture.execution_registry.attach_context.assert_called_once_with(
        job_id="job-1",
        context=fixture.context,
    )
    fixture.job_handler_loader.load.assert_called_once_with()


@pytest.mark.parametrize("error_message", ["", "   "])
def test_job_preparation_handler_propagates_blank_input_preparation_error(
    error_message: str,
) -> None:
    fixture = _fixture()
    workspace = _workspace()

    fixture.input_artifact_preparer.prepare.side_effect = RuntimeError(error_message)

    with pytest.raises(RuntimeError):
        fixture.handler.prepare(
            workspace=workspace,
        )

    fixture.input_artifact_preparer.prepare.assert_called_once_with(workspace)

    fixture.context_factory.create.assert_not_called()
    fixture.execution_registry.attach_context.assert_not_called()
    fixture.job_handler_loader.load.assert_not_called()


def test_job_preparation_handler_does_not_catch_base_exception() -> None:
    fixture = _fixture()
    workspace = _workspace()

    fixture.input_artifact_preparer.prepare.side_effect = SystemExit(
        "shutdown requested"
    )

    with pytest.raises(SystemExit, match="shutdown requested"):
        fixture.handler.prepare(
            workspace=workspace,
        )

    fixture.context_factory.create.assert_not_called()
    fixture.execution_registry.attach_context.assert_not_called()
    fixture.job_handler_loader.load.assert_not_called()


@pytest.mark.parametrize(
    ("field_name", "field_value", "error_message"),
    [
        (
            "execution_registry",
            None,
            "execution_registry cannot be null.",
        ),
        (
            "input_artifact_preparer",
            None,
            "input_artifact_preparer cannot be null.",
        ),
        (
            "context_factory",
            None,
            "context_factory cannot be null.",
        ),
        (
            "job_handler_loader",
            None,
            "job_handler_loader cannot be null.",
        ),
    ],
)
def test_job_preparation_handler_rejects_invalid_constructor_arguments(
    field_name: str,
    field_value: Any,
    error_message: str,
) -> None:
    kwargs = _constructor_kwargs()
    kwargs[field_name] = field_value

    with pytest.raises(ValueError, match=error_message):
        JobPreparationHandler(**kwargs)


def test_prepare_rejects_null_workspace() -> None:
    fixture = _fixture()

    with pytest.raises(ValueError, match="workspace cannot be null."):
        fixture.handler.prepare(
            workspace=cast(JobWorkspace, cast(object, None)),
        )

    fixture.input_artifact_preparer.prepare.assert_not_called()
    fixture.context_factory.create.assert_not_called()
    fixture.execution_registry.attach_context.assert_not_called()
    fixture.job_handler_loader.load.assert_not_called()


def _fixture() -> SimpleNamespace:
    execution_registry = MagicMock(name="execution_registry")
    input_artifact_preparer = MagicMock(name="input_artifact_preparer")
    context_factory = MagicMock(name="context_factory")
    job_handler_loader = MagicMock(name="job_handler_loader")

    prepared_job_inputs = MagicMock(name="prepared_job_inputs")
    context = MagicMock(name="context")
    job_handler = MagicMock(name="job_handler")

    input_artifact_preparer.prepare.return_value = prepared_job_inputs
    context_factory.create.return_value = context
    job_handler_loader.load.return_value = job_handler

    handler = JobPreparationHandler(
        execution_registry=execution_registry,
        input_artifact_preparer=input_artifact_preparer,
        context_factory=context_factory,
        job_handler_loader=job_handler_loader,
    )

    return SimpleNamespace(
        handler=handler,
        execution_registry=execution_registry,
        input_artifact_preparer=input_artifact_preparer,
        context_factory=context_factory,
        job_handler_loader=job_handler_loader,
        prepared_job_inputs=prepared_job_inputs,
        context=context,
        job_handler=job_handler,
    )


def _constructor_kwargs() -> dict[str, Any]:
    return {
        "execution_registry": MagicMock(name="execution_registry"),
        "input_artifact_preparer": MagicMock(name="input_artifact_preparer"),
        "context_factory": MagicMock(name="context_factory"),
        "job_handler_loader": MagicMock(name="job_handler_loader"),
    }


def _workspace() -> JobWorkspace:
    manifest = _manifest()
    work_dir = Path("/tmp/jobs/42/job-1")

    return JobWorkspace(
        manifest=manifest,
        work_dir=work_dir,
        input_dir=work_dir / "in",
        output_dir=work_dir / "out",
        worker_id="worker-1",
    )


def _manifest() -> JobManifest:
    return JobManifest(
        manifest_version=1,
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        inputs={
            "number_a": ArtifactRef(
                object_key="jobs/42/job-1/in/number_a.csv",
                format=ArtifactFormat.CSV,
            ),
        },
        params={},
        outputs={},
    )

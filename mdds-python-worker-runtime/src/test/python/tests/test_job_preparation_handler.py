# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.execution.job_preparation_handler import (
    JobPreparationHandler,
    PreparedJob,
)


def test_job_preparation_handler_returns_prepared_job_when_preparation_succeeds() -> (
    None
):
    fixture = _fixture()
    manifest = _manifest()
    submitted_ack = MagicMock(name="submitted_ack")

    result = fixture.handler.prepare_or_handle_failure(
        manifest_object_key="jobs/42/job-1/manifest.json",
        manifest=manifest,
        submitted_ack=submitted_ack,
    )

    assert isinstance(result, PreparedJob)
    assert result.context is fixture.context
    assert result.handler is fixture.job_handler

    fixture.input_artifact_preparer.prepare.assert_called_once_with(
        42,
        "job-1",
        manifest.inputs,
    )
    fixture.context_factory.create.assert_called_once_with(
        manifest,
        fixture.prepared_job_inputs,
    )
    fixture.job_handler_loader.load.assert_called_once_with()

    fixture.status_publisher.publish_error.assert_not_called()
    submitted_ack.ack.assert_not_called()


def test_job_preparation_handler_handles_input_preparation_failure_as_error() -> None:
    fixture = _fixture()
    manifest = _manifest()
    submitted_ack = MagicMock(name="submitted_ack")
    events: list[str] = []

    fixture.input_artifact_preparer.prepare.side_effect = RuntimeError(
        "Cannot download input artifact."
    )
    fixture.status_publisher.publish_error.side_effect = (
        lambda **_kwargs: events.append("publish_error")
    )

    def ack_side_effect() -> None:
        assert events == ["publish_error"]
        events.append("ack")

    submitted_ack.ack.side_effect = ack_side_effect

    result = fixture.handler.prepare_or_handle_failure(
        manifest_object_key="jobs/42/job-1/manifest.json",
        manifest=manifest,
        submitted_ack=submitted_ack,
    )

    assert result is None
    assert events == ["publish_error", "ack"]

    fixture.status_publisher.publish_error.assert_called_once_with(
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        worker_id="worker-1",
        message="Cannot download input artifact.",
    )
    submitted_ack.ack.assert_called_once_with()

    fixture.context_factory.create.assert_not_called()
    fixture.job_handler_loader.load.assert_not_called()


def test_job_preparation_handler_handles_context_creation_failure_as_error() -> None:
    fixture = _fixture()
    manifest = _manifest()
    submitted_ack = MagicMock(name="submitted_ack")
    events: list[str] = []

    fixture.context_factory.create.side_effect = RuntimeError(
        "Cannot save context snapshot."
    )
    fixture.status_publisher.publish_error.side_effect = (
        lambda **_kwargs: events.append("publish_error")
    )

    def ack_side_effect() -> None:
        assert events == ["publish_error"]
        events.append("ack")

    submitted_ack.ack.side_effect = ack_side_effect

    result = fixture.handler.prepare_or_handle_failure(
        manifest_object_key="jobs/42/job-1/manifest.json",
        manifest=manifest,
        submitted_ack=submitted_ack,
    )

    assert result is None
    assert events == ["publish_error", "ack"]

    fixture.input_artifact_preparer.prepare.assert_called_once_with(
        42,
        "job-1",
        manifest.inputs,
    )
    fixture.context_factory.create.assert_called_once_with(
        manifest,
        fixture.prepared_job_inputs,
    )
    fixture.job_handler_loader.load.assert_not_called()

    fixture.status_publisher.publish_error.assert_called_once_with(
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        worker_id="worker-1",
        message="Cannot save context snapshot.",
    )
    submitted_ack.ack.assert_called_once_with()


def test_job_preparation_handler_handles_handler_loading_failure_as_error() -> None:
    fixture = _fixture()
    manifest = _manifest()
    submitted_ack = MagicMock(name="submitted_ack")
    events: list[str] = []

    fixture.job_handler_loader.load.side_effect = RuntimeError(
        "Cannot load job handler."
    )
    fixture.status_publisher.publish_error.side_effect = (
        lambda **_kwargs: events.append("publish_error")
    )

    def ack_side_effect() -> None:
        assert events == ["publish_error"]
        events.append("ack")

    submitted_ack.ack.side_effect = ack_side_effect

    result = fixture.handler.prepare_or_handle_failure(
        manifest_object_key="jobs/42/job-1/manifest.json",
        manifest=manifest,
        submitted_ack=submitted_ack,
    )

    assert result is None
    assert events == ["publish_error", "ack"]

    fixture.input_artifact_preparer.prepare.assert_called_once_with(
        42,
        "job-1",
        manifest.inputs,
    )
    fixture.context_factory.create.assert_called_once_with(
        manifest,
        fixture.prepared_job_inputs,
    )
    fixture.job_handler_loader.load.assert_called_once_with()

    fixture.status_publisher.publish_error.assert_called_once_with(
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        worker_id="worker-1",
        message="Cannot load job handler.",
    )
    submitted_ack.ack.assert_called_once_with()


@pytest.mark.parametrize("error_message", ["", "   "])
def test_job_preparation_handler_uses_fallback_message_when_error_message_is_blank(
    error_message: str,
) -> None:
    fixture = _fixture()
    manifest = _manifest()
    submitted_ack = MagicMock(name="submitted_ack")

    fixture.input_artifact_preparer.prepare.side_effect = RuntimeError(error_message)

    result = fixture.handler.prepare_or_handle_failure(
        manifest_object_key="jobs/42/job-1/manifest.json",
        manifest=manifest,
        submitted_ack=submitted_ack,
    )

    assert result is None

    fixture.status_publisher.publish_error.assert_called_once_with(
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        worker_id="worker-1",
        message="Worker-side job preparation failed.",
    )
    submitted_ack.ack.assert_called_once_with()

    fixture.context_factory.create.assert_not_called()
    fixture.job_handler_loader.load.assert_not_called()


def test_job_preparation_handler_does_not_ack_when_error_publication_fails() -> None:
    fixture = _fixture()
    manifest = _manifest()
    submitted_ack = MagicMock(name="submitted_ack")

    fixture.input_artifact_preparer.prepare.side_effect = RuntimeError(
        "Cannot download input artifact."
    )
    fixture.status_publisher.publish_error.side_effect = RuntimeError(
        "status publish failed"
    )

    with pytest.raises(RuntimeError, match="status publish failed"):
        fixture.handler.prepare_or_handle_failure(
            manifest_object_key="jobs/42/job-1/manifest.json",
            manifest=manifest,
            submitted_ack=submitted_ack,
        )

    fixture.status_publisher.publish_error.assert_called_once_with(
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        worker_id="worker-1",
        message="Cannot download input artifact.",
    )
    submitted_ack.ack.assert_not_called()

    fixture.context_factory.create.assert_not_called()
    fixture.job_handler_loader.load.assert_not_called()


def test_job_preparation_handler_does_not_catch_base_exception() -> None:
    fixture = _fixture()
    manifest = _manifest()
    submitted_ack = MagicMock(name="submitted_ack")

    fixture.input_artifact_preparer.prepare.side_effect = SystemExit(
        "shutdown requested"
    )

    with pytest.raises(SystemExit, match="shutdown requested"):
        fixture.handler.prepare_or_handle_failure(
            manifest_object_key="jobs/42/job-1/manifest.json",
            manifest=manifest,
            submitted_ack=submitted_ack,
        )

    fixture.status_publisher.publish_error.assert_not_called()
    submitted_ack.ack.assert_not_called()

    fixture.context_factory.create.assert_not_called()
    fixture.job_handler_loader.load.assert_not_called()


@pytest.mark.parametrize(
    ("field_name", "field_value", "error_message"),
    [
        (
            "input_artifact_preparer",
            None,
            "input_artifact_preparer cannot be null.",
        ),
        ("context_factory", None, "context_factory cannot be null."),
        ("job_handler_loader", None, "job_handler_loader cannot be null."),
        ("status_publisher", None, "status_publisher cannot be null."),
        ("worker_id", None, "worker_id cannot be null or blank."),
        ("worker_id", "", "worker_id cannot be null or blank."),
        ("worker_id", " ", "worker_id cannot be null or blank."),
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


@pytest.mark.parametrize(
    ("field_name", "field_value", "error_message"),
    [
        (
            "manifest_object_key",
            None,
            "manifest_object_key cannot be null or blank.",
        ),
        (
            "manifest_object_key",
            "",
            "manifest_object_key cannot be null or blank.",
        ),
        (
            "manifest_object_key",
            " ",
            "manifest_object_key cannot be null or blank.",
        ),
        ("manifest", None, "manifest cannot be null."),
        ("submitted_ack", None, "submitted_ack cannot be null."),
    ],
)
def test_prepare_or_handle_failure_rejects_invalid_arguments_without_publishing_error(
    field_name: str,
    field_value: Any,
    error_message: str,
) -> None:
    fixture = _fixture()
    submitted_ack = MagicMock(name="submitted_ack")

    kwargs: dict[str, Any] = {
        "manifest_object_key": "jobs/42/job-1/manifest.json",
        "manifest": _manifest(),
        "submitted_ack": submitted_ack,
        field_name: field_value,
    }

    with pytest.raises(ValueError, match=error_message):
        fixture.handler.prepare_or_handle_failure(**kwargs)

    fixture.status_publisher.publish_error.assert_not_called()
    submitted_ack.ack.assert_not_called()

    fixture.input_artifact_preparer.prepare.assert_not_called()
    fixture.context_factory.create.assert_not_called()
    fixture.job_handler_loader.load.assert_not_called()


def _fixture() -> SimpleNamespace:
    input_artifact_preparer = MagicMock(name="input_artifact_preparer")
    context_factory = MagicMock(name="context_factory")
    job_handler_loader = MagicMock(name="job_handler_loader")
    status_publisher = MagicMock(name="status_publisher")

    prepared_job_inputs = MagicMock(name="prepared_job_inputs")
    context = MagicMock(name="context")
    job_handler = MagicMock(name="job_handler")

    input_artifact_preparer.prepare.return_value = prepared_job_inputs
    context_factory.create.return_value = context
    job_handler_loader.load.return_value = job_handler

    handler = JobPreparationHandler(
        input_artifact_preparer=input_artifact_preparer,
        context_factory=context_factory,
        job_handler_loader=job_handler_loader,
        status_publisher=status_publisher,
        worker_id="worker-1",
    )

    return SimpleNamespace(
        handler=handler,
        input_artifact_preparer=input_artifact_preparer,
        context_factory=context_factory,
        job_handler_loader=job_handler_loader,
        status_publisher=status_publisher,
        prepared_job_inputs=prepared_job_inputs,
        context=context,
        job_handler=job_handler,
    )


def _constructor_kwargs() -> dict[str, Any]:
    return {
        "input_artifact_preparer": MagicMock(name="input_artifact_preparer"),
        "context_factory": MagicMock(name="context_factory"),
        "job_handler_loader": MagicMock(name="job_handler_loader"),
        "status_publisher": MagicMock(name="status_publisher"),
        "worker_id": "worker-1",
    }


def _manifest() -> MagicMock:
    manifest = MagicMock(name="manifest")
    manifest.user_id = 42
    manifest.job_id = "job-1"
    manifest.job_type = "SOLVING_SLAE"
    manifest.inputs = {"number_a": MagicMock(name="number_a_ref")}
    return manifest

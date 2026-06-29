# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.execution.validation_handler import (
    ValidationFailed,
    ValidationHandler,
)


def test_validation_handler_returns_true_when_handler_validation_succeeds(
    tmp_path: Path,
) -> None:
    status_publisher = MagicMock()
    validation_handler = ValidationHandler(status_publisher, "worker-1")

    handler = MagicMock()
    context = _context(tmp_path)
    manifest = _manifest()
    submitted_ack = MagicMock()

    result = validation_handler.validate_or_handle_failure(
        handler=handler,
        context=context,
        manifest=manifest,
        submitted_ack=submitted_ack,
    )

    assert result is True

    handler.validate.assert_called_once_with(context)
    status_publisher.publish_validation_failed.assert_not_called()
    submitted_ack.ack.assert_not_called()


def test_validation_handler_publishes_validation_failed_when_validation_fails(
    tmp_path: Path,
) -> None:
    status_publisher = MagicMock()
    validation_handler = ValidationHandler(status_publisher, "worker-1")

    handler = MagicMock()
    handler.validate.side_effect = ValidationFailed("Invalid matrix format.")

    context = _context(tmp_path)
    context.work_dir.mkdir(parents=True)
    manifest = _manifest()
    submitted_ack = MagicMock()

    result = validation_handler.validate_or_handle_failure(
        handler=handler,
        context=context,
        manifest=manifest,
        submitted_ack=submitted_ack,
    )

    assert result is False

    status_publisher.publish_validation_failed.assert_called_once_with(
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        worker_id="worker-1",
        message="Invalid matrix format.",
    )


def test_validation_handler_acks_after_successful_validation_failed_publication(
    tmp_path: Path,
) -> None:
    events: list[str] = []

    status_publisher = MagicMock()
    status_publisher.publish_validation_failed.side_effect = (
        lambda **_kwargs: events.append("publish_validation_failed")
    )

    validation_handler = ValidationHandler(status_publisher, "worker-1")

    handler = MagicMock()
    handler.validate.side_effect = ValidationFailed("Invalid matrix format.")

    context = _context(tmp_path)
    context.work_dir.mkdir(parents=True)

    manifest = _manifest()

    submitted_ack = MagicMock()
    submitted_ack.ack.side_effect = lambda: events.append("ack")

    result = validation_handler.validate_or_handle_failure(
        handler=handler,
        context=context,
        manifest=manifest,
        submitted_ack=submitted_ack,
    )

    assert result is False
    assert events == ["publish_validation_failed", "ack"]


def test_validation_handler_removes_local_workspace_after_validation_failure(
    tmp_path: Path,
) -> None:
    status_publisher = MagicMock()
    validation_handler = ValidationHandler(status_publisher, "worker-1")

    handler = MagicMock()
    handler.validate.side_effect = ValidationFailed("Invalid matrix format.")

    context = _context(tmp_path)
    context.work_dir.mkdir(parents=True)
    (context.work_dir / "input.csv").write_text("bad-input", encoding="utf-8")

    manifest = _manifest()
    submitted_ack = MagicMock()

    result = validation_handler.validate_or_handle_failure(
        handler=handler,
        context=context,
        manifest=manifest,
        submitted_ack=submitted_ack,
    )

    assert result is False
    assert not context.work_dir.exists()


def test_validation_handler_returns_false_after_validation_failure_is_handled(
    tmp_path: Path,
) -> None:
    status_publisher = MagicMock()
    validation_handler = ValidationHandler(status_publisher, "worker-1")

    handler = MagicMock()
    handler.validate.side_effect = ValidationFailed("Invalid matrix format.")

    context = _context(tmp_path)
    context.work_dir.mkdir(parents=True)

    result = validation_handler.validate_or_handle_failure(
        handler=handler,
        context=context,
        manifest=_manifest(),
        submitted_ack=MagicMock(),
    )

    assert result is False


def test_validation_handler_does_not_ack_when_validation_failed_publication_fails(
    tmp_path: Path,
) -> None:
    status_publisher = MagicMock()
    status_publisher.publish_validation_failed.side_effect = RuntimeError(
        "status publish failed"
    )

    validation_handler = ValidationHandler(status_publisher, "worker-1")

    handler = MagicMock()
    handler.validate.side_effect = ValidationFailed("Invalid matrix format.")

    context = _context(tmp_path)
    context.work_dir.mkdir(parents=True)

    submitted_ack = MagicMock()

    with pytest.raises(RuntimeError, match="status publish failed"):
        validation_handler.validate_or_handle_failure(
            handler=handler,
            context=context,
            manifest=_manifest(),
            submitted_ack=submitted_ack,
        )

    submitted_ack.ack.assert_not_called()
    assert context.work_dir.exists()


@pytest.mark.parametrize(
    "unexpected_error",
    [
        RuntimeError("unexpected runtime error"),
        ValueError("unexpected value error"),
    ],
)
def test_validation_handler_does_not_catch_unexpected_validation_errors(
    tmp_path: Path,
    unexpected_error: Exception,
) -> None:
    status_publisher = MagicMock()
    validation_handler = ValidationHandler(status_publisher, "worker-1")

    handler = MagicMock()
    handler.validate.side_effect = unexpected_error

    context = _context(tmp_path)
    context.work_dir.mkdir(parents=True)

    submitted_ack = MagicMock()

    with pytest.raises(type(unexpected_error), match=str(unexpected_error)):
        validation_handler.validate_or_handle_failure(
            handler=handler,
            context=context,
            manifest=_manifest(),
            submitted_ack=submitted_ack,
        )

    status_publisher.publish_validation_failed.assert_not_called()
    submitted_ack.ack.assert_not_called()
    assert context.work_dir.exists()


def test_validation_handler_rejects_null_status_publisher() -> None:
    with pytest.raises(ValueError, match="status_publisher cannot be null."):
        ValidationHandler(None, "worker-1")


@pytest.mark.parametrize("worker_id", [None, "", " "])
def test_validation_handler_rejects_null_or_blank_worker_id(
    worker_id: str | None,
) -> None:
    with pytest.raises(ValueError, match="worker_id cannot be null or blank."):
        ValidationHandler(MagicMock(), worker_id)


@pytest.mark.parametrize(
    ("field_name", "field_value", "error_message"),
    [
        ("handler", None, "handler cannot be null."),
        ("context", None, "context cannot be null."),
        ("manifest", None, "manifest cannot be null."),
        ("submitted_ack", None, "submitted_ack cannot be null."),
    ],
)
def test_validate_or_handle_failure_rejects_null_arguments(
    tmp_path: Path,
    field_name: str,
    field_value: Any,
    error_message: str,
) -> None:
    validation_handler = ValidationHandler(MagicMock(), "worker-1")

    kwargs: dict[str, Any] = {
        "handler": MagicMock(),
        "context": _context(tmp_path),
        "manifest": _manifest(),
        "submitted_ack": MagicMock(),
        field_name: field_value,
    }

    with pytest.raises(ValueError, match=error_message):
        validation_handler.validate_or_handle_failure(**kwargs)


def _manifest() -> MagicMock:
    manifest = MagicMock()
    manifest.user_id = 42
    manifest.job_id = "job-1"
    manifest.job_type = "SOLVING_SLAE"
    return manifest


def _context(tmp_path: Path) -> MagicMock:
    context = MagicMock()
    context.user_id = 42
    context.job_id = "job-1"
    context.job_type = "SOLVING_SLAE"
    context.work_dir = tmp_path / "jobs" / "42" / "job-1"
    return context


def test_validation_handler_handles_already_absent_local_workspace(
    tmp_path: Path,
) -> None:
    status_publisher = MagicMock()
    validation_handler = ValidationHandler(status_publisher, "worker-1")

    handler = MagicMock()
    handler.validate.side_effect = ValidationFailed("Invalid matrix format.")

    context = _context(tmp_path)

    assert not context.work_dir.exists()

    submitted_ack = MagicMock()

    result = validation_handler.validate_or_handle_failure(
        handler=handler,
        context=context,
        manifest=_manifest(),
        submitted_ack=submitted_ack,
    )

    assert result is False

    status_publisher.publish_validation_failed.assert_called_once_with(
        user_id=42,
        job_id="job-1",
        job_type="SOLVING_SLAE",
        worker_id="worker-1",
        message="Invalid matrix format.",
    )

    submitted_ack.ack.assert_called_once_with()
    assert not context.work_dir.exists()


def test_validation_handler_suppresses_cleanup_failure_when_workspace_is_file(
    tmp_path: Path,
) -> None:
    status_publisher = MagicMock()
    validation_handler = ValidationHandler(status_publisher, "worker-1")

    handler = MagicMock()
    handler.validate.side_effect = ValidationFailed("Invalid matrix format.")

    context = _context(tmp_path)
    context.work_dir.parent.mkdir(parents=True)
    context.work_dir.write_text("not-a-directory", encoding="utf-8")

    submitted_ack = MagicMock()

    result = validation_handler.validate_or_handle_failure(
        handler=handler,
        context=context,
        manifest=_manifest(),
        submitted_ack=submitted_ack,
    )

    assert result is False

    status_publisher.publish_validation_failed.assert_called_once()
    submitted_ack.ack.assert_called_once_with()

    assert context.work_dir.exists()
    assert context.work_dir.is_file()


def test_validation_handler_suppresses_cleanup_failure_when_rmtree_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status_publisher = MagicMock()
    validation_handler = ValidationHandler(status_publisher, "worker-1")

    handler = MagicMock()
    handler.validate.side_effect = ValidationFailed("Invalid matrix format.")

    context = _context(tmp_path)
    context.work_dir.mkdir(parents=True)
    (context.work_dir / "input.csv").write_text("bad-input", encoding="utf-8")

    def fail_rmtree(_path: Path) -> None:
        raise RuntimeError("cleanup failed")

    monkeypatch.setattr(
        "mdds_worker_runtime.execution.validation_handler.shutil.rmtree",
        fail_rmtree,
    )

    submitted_ack = MagicMock()

    result = validation_handler.validate_or_handle_failure(
        handler=handler,
        context=context,
        manifest=_manifest(),
        submitted_ack=submitted_ack,
    )

    assert result is False

    status_publisher.publish_validation_failed.assert_called_once()
    submitted_ack.ack.assert_called_once_with()

    assert context.work_dir.exists()

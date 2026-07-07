# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
from unittest.mock import MagicMock

import pytest

from mdds_worker_runtime.execution.validation_handler import (
    ValidationFailed,
    ValidationHandler,
)


def test_validation_handler_delegates_to_job_handler_validate() -> None:
    handler = MagicMock()
    context = MagicMock()

    ValidationHandler().validate(
        handler=handler,
        context=context,
    )

    handler.validate.assert_called_once_with(context)


def test_validation_handler_propagates_validation_failed() -> None:
    handler = MagicMock()
    context = MagicMock()
    handler.validate.side_effect = ValidationFailed("invalid job")

    with pytest.raises(ValidationFailed, match="invalid job"):
        ValidationHandler().validate(
            handler=handler,
            context=context,
        )


def test_validation_handler_propagates_unexpected_exception() -> None:
    handler = MagicMock()
    context = MagicMock()
    handler.validate.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        ValidationHandler().validate(
            handler=handler,
            context=context,
        )


def test_validation_handler_rejects_null_handler() -> None:
    with pytest.raises(ValueError, match="handler cannot be null."):
        ValidationHandler().validate(
            handler=None,
            context=MagicMock(),
        )


def test_validation_handler_rejects_null_context() -> None:
    with pytest.raises(ValueError, match="context cannot be null."):
        ValidationHandler().validate(
            handler=MagicMock(),
            context=None,
        )

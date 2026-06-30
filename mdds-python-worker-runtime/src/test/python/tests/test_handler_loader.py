# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import pytest

from mdds_worker_runtime.execution.handler import JobHandler
from mdds_worker_runtime.execution.handler_loader import (
    JobHandlerLoader,
    JobHandlerLoadError,
)
from tests.fixtures.job_handlers import (
    ValidJobHandler,
    ValidUnannotatedJobHandler,
    VarArgsConstructorJobHandler,
    OptionalConstructorArgumentJobHandler,
)

FIXTURE_MODULE = "tests.fixtures.job_handlers"


def test_loader_loads_valid_job_handler() -> None:
    loader = JobHandlerLoader(f"{FIXTURE_MODULE}:ValidJobHandler")

    handler = loader.load()

    assert isinstance(handler, ValidJobHandler)
    assert isinstance(handler, JobHandler)


def test_loader_loads_valid_unannotated_job_handler() -> None:
    loader = JobHandlerLoader(f"{FIXTURE_MODULE}:ValidUnannotatedJobHandler")

    handler = loader.load()

    assert isinstance(handler, ValidUnannotatedJobHandler)
    assert isinstance(handler, JobHandler)


def test_loader_returns_fresh_handler_instance_on_each_load() -> None:
    loader = JobHandlerLoader(f"{FIXTURE_MODULE}:ValidJobHandler")

    first_handler = loader.load()
    second_handler = loader.load()

    assert isinstance(first_handler, ValidJobHandler)
    assert isinstance(second_handler, ValidJobHandler)
    assert first_handler is not second_handler


def test_loader_rejects_null_import_path() -> None:
    with pytest.raises(
        ValueError, match="handler import path cannot be null or blank."
    ):
        JobHandlerLoader(None)  # type: ignore[arg-type]


def test_loader_rejects_blank_import_path() -> None:
    with pytest.raises(
        ValueError, match="handler import path cannot be null or blank."
    ):
        JobHandlerLoader(" ")


def test_loader_rejects_import_path_without_colon() -> None:
    with pytest.raises(
        JobHandlerLoadError,
        match="Handler import path must have format '<module>:<class>'.",
    ):
        JobHandlerLoader("tests.fixtures.job_handlers.ValidJobHandler")


def test_loader_rejects_blank_module_name() -> None:
    with pytest.raises(
        JobHandlerLoadError,
        match="Handler module name cannot be blank.",
    ):
        JobHandlerLoader(":ValidJobHandler")


def test_loader_rejects_blank_class_name() -> None:
    with pytest.raises(
        JobHandlerLoadError,
        match="Handler class name cannot be blank.",
    ):
        JobHandlerLoader(f"{FIXTURE_MODULE}: ")


def test_loader_rejects_nested_class_name() -> None:
    with pytest.raises(
        JobHandlerLoadError,
        match="Nested handler class names are not supported.",
    ):
        JobHandlerLoader(f"{FIXTURE_MODULE}:Outer.Inner")


def test_loader_rejects_missing_module() -> None:
    with pytest.raises(
        JobHandlerLoadError,
        match="Cannot import handler module 'tests.fixtures.missing_module'",
    ):
        JobHandlerLoader("tests.fixtures.missing_module:ValidJobHandler")


def test_loader_rejects_missing_handler_class() -> None:
    with pytest.raises(
        JobHandlerLoadError,
        match="Handler class 'MissingJobHandler' was not found",
    ):
        JobHandlerLoader(f"{FIXTURE_MODULE}:MissingJobHandler")


@pytest.mark.parametrize(
    "attribute_name",
    [
        "handler_factory",
        "not_a_handler_class",
    ],
)
def test_loader_rejects_imported_attribute_that_is_not_class(
    attribute_name: str,
) -> None:
    with pytest.raises(
        JobHandlerLoadError,
        match="Handler import path must point to a JobHandler class",
    ):
        JobHandlerLoader(f"{FIXTURE_MODULE}:{attribute_name}")


def test_loader_rejects_class_that_does_not_inherit_from_job_handler() -> None:
    with pytest.raises(
        JobHandlerLoadError,
        match="Handler class must inherit from JobHandler",
    ):
        JobHandlerLoader(f"{FIXTURE_MODULE}:PlainClass")


def test_loader_rejects_abstract_job_handler_class() -> None:
    with pytest.raises(
        JobHandlerLoadError,
        match="Handler class must implement all abstract methods",
    ):
        JobHandlerLoader(f"{FIXTURE_MODULE}:AbstractJobHandler")


def test_loader_rejects_base_job_handler_class() -> None:
    with pytest.raises(
        JobHandlerLoadError,
        match="Handler class must implement all abstract methods",
    ):
        JobHandlerLoader("mdds_worker_runtime.execution.handler:JobHandler")


def test_loader_rejects_handler_with_required_constructor_argument() -> None:
    with pytest.raises(
        JobHandlerLoadError,
        match="Handler class must have a no-argument constructor",
    ):
        JobHandlerLoader(f"{FIXTURE_MODULE}:ConstructorArgumentJobHandler")


def test_loader_wraps_handler_constructor_failure() -> None:
    loader = JobHandlerLoader(f"{FIXTURE_MODULE}:ConstructorRaisesJobHandler")

    with pytest.raises(
        JobHandlerLoadError,
        match="Cannot instantiate handler class while loading",
    ):
        loader.load()


@pytest.mark.parametrize(
    ("class_name", "expected_message"),
    [
        (
            "ValidateWithoutContextJobHandler",
            "Handler method validate must have signature",
        ),
        (
            "ExecuteWithoutContextJobHandler",
            "Handler method execute must have signature",
        ),
    ],
)
def test_loader_rejects_handler_method_without_context_parameter(
    class_name: str,
    expected_message: str,
) -> None:
    with pytest.raises(JobHandlerLoadError, match=expected_message):
        JobHandlerLoader(f"{FIXTURE_MODULE}:{class_name}")


@pytest.mark.parametrize(
    ("class_name", "expected_message"),
    [
        (
            "ValidateWithKeywordOnlyContextJobHandler",
            "Handler method validate context parameter must be positional",
        ),
        (
            "ExecuteWithKeywordOnlyContextJobHandler",
            "Handler method execute context parameter must be positional",
        ),
    ],
)
def test_loader_rejects_handler_method_with_keyword_only_context_parameter(
    class_name: str,
    expected_message: str,
) -> None:
    with pytest.raises(JobHandlerLoadError, match=expected_message):
        JobHandlerLoader(f"{FIXTURE_MODULE}:{class_name}")


@pytest.mark.parametrize(
    ("class_name", "expected_message"),
    [
        (
            "ValidateWithWrongContextAnnotationJobHandler",
            "Handler method validate context parameter must be annotated",
        ),
        (
            "ExecuteWithWrongContextAnnotationJobHandler",
            "Handler method execute context parameter must be annotated",
        ),
    ],
)
def test_loader_rejects_handler_method_with_wrong_context_annotation(
    class_name: str,
    expected_message: str,
) -> None:
    with pytest.raises(JobHandlerLoadError, match=expected_message):
        JobHandlerLoader(f"{FIXTURE_MODULE}:{class_name}")


@pytest.mark.parametrize(
    ("class_name", "expected_message"),
    [
        (
            "ValidateWithWrongReturnAnnotationJobHandler",
            "Handler method validate return type must be None",
        ),
        (
            "ExecuteWithWrongReturnAnnotationJobHandler",
            "Handler method execute return type must be None",
        ),
    ],
)
def test_loader_rejects_handler_method_with_wrong_return_annotation(
    class_name: str,
    expected_message: str,
) -> None:
    with pytest.raises(JobHandlerLoadError, match=expected_message):
        JobHandlerLoader(f"{FIXTURE_MODULE}:{class_name}")


def test_loader_loads_handler_with_varargs_constructor() -> None:
    loader = JobHandlerLoader(f"{FIXTURE_MODULE}:VarArgsConstructorJobHandler")

    handler = loader.load()

    assert isinstance(handler, VarArgsConstructorJobHandler)
    assert isinstance(handler, JobHandler)


def test_loader_loads_handler_with_optional_constructor_argument() -> None:
    loader = JobHandlerLoader(f"{FIXTURE_MODULE}:OptionalConstructorArgumentJobHandler")

    handler = loader.load()

    assert isinstance(handler, OptionalConstructorArgumentJobHandler)
    assert handler.value == "default"
    assert isinstance(handler, JobHandler)


def test_loader_rejects_handler_with_invalid_constructor_signature() -> None:
    with pytest.raises(
        JobHandlerLoadError,
        match="Cannot inspect handler constructor",
    ):
        JobHandlerLoader(f"{FIXTURE_MODULE}:InvalidConstructorSignatureJobHandler")


def test_loader_rejects_handler_constructor_returning_non_job_handler_instance() -> (
    None
):
    loader = JobHandlerLoader(
        f"{FIXTURE_MODULE}:ConstructorReturnsNonJobHandlerInstanceJobHandler"
    )

    with pytest.raises(
        JobHandlerLoadError,
        match="Handler constructor must return a JobHandler instance",
    ):
        loader.load()


def test_loader_validate_loadable_rejects_non_job_handler_instance() -> None:
    loader = JobHandlerLoader(
        f"{FIXTURE_MODULE}:ConstructorReturnsNonJobHandlerInstanceJobHandler"
    )

    with pytest.raises(
        JobHandlerLoadError,
        match="Handler constructor must return a JobHandler instance",
    ):
        loader.validate_loadable()

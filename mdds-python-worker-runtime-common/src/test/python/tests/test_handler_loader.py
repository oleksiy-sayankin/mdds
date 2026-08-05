# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

import pytest
from mdds_worker_runtime_common.execution.handler import WorkerHandler
from mdds_worker_runtime_common.execution.handler_loader import (
    WorkerHandlerLoadError,
    load_worker_handler,
)

from tests.fixtures.worker_handlers import (
    OptionalConstructorArgumentWorkerHandler,
    ValidPositionalOnlyContextWorkerHandler,
    ValidUnannotatedWorkerHandler,
    ValidWorkerHandler,
    VarArgsConstructorWorkerHandler,
)

FIXTURE_MODULE = "tests.fixtures.worker_handlers"
POSTPONED_ANNOTATIONS_FIXTURE_MODULE = (
    "tests.fixtures.postponed_annotation_worker_handlers"
)


def test_loads_valid_worker_handler() -> None:
    handler = load_worker_handler(f"{FIXTURE_MODULE}:ValidWorkerHandler")

    assert isinstance(handler, ValidWorkerHandler)
    assert isinstance(handler, WorkerHandler)


def test_loads_valid_unannotated_worker_handler() -> None:
    handler = load_worker_handler(f"{FIXTURE_MODULE}:ValidUnannotatedWorkerHandler")

    assert isinstance(handler, ValidUnannotatedWorkerHandler)
    assert isinstance(handler, WorkerHandler)


def test_loads_handler_with_positional_only_context_parameter() -> None:
    handler = load_worker_handler(
        f"{FIXTURE_MODULE}:ValidPositionalOnlyContextWorkerHandler"
    )

    assert isinstance(handler, ValidPositionalOnlyContextWorkerHandler)
    assert isinstance(handler, WorkerHandler)


def test_returns_fresh_worker_handler_instance_on_each_load() -> None:
    import_path = f"{FIXTURE_MODULE}:ValidWorkerHandler"

    first_handler = load_worker_handler(import_path)
    second_handler = load_worker_handler(import_path)

    assert isinstance(first_handler, ValidWorkerHandler)
    assert isinstance(second_handler, ValidWorkerHandler)
    assert first_handler is not second_handler


def test_normalizes_whitespace_around_import_path_parts() -> None:
    handler = load_worker_handler(f"  {FIXTURE_MODULE} : ValidWorkerHandler  ")

    assert isinstance(handler, ValidWorkerHandler)


@pytest.mark.parametrize("import_path", [None, "", " \t "])
def test_rejects_null_or_blank_import_path(import_path: str | None) -> None:
    with pytest.raises(
        ValueError,
        match="handler import path cannot be null or blank.",
    ):
        load_worker_handler(import_path)  # type: ignore[arg-type]


def test_rejects_import_path_without_colon() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler import path must have format '<module>:<class>'.",
    ):
        load_worker_handler("tests.fixtures.worker_handlers.ValidWorkerHandler")


def test_rejects_blank_module_name() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler module name cannot be blank.",
    ):
        load_worker_handler(":ValidWorkerHandler")


def test_rejects_blank_class_name() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler class name cannot be blank.",
    ):
        load_worker_handler(f"{FIXTURE_MODULE}: ")


def test_rejects_nested_class_name() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Nested handler class names are not supported.",
    ):
        load_worker_handler(f"{FIXTURE_MODULE}:Outer.Inner")


def test_rejects_missing_module() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Cannot import handler module 'tests.fixtures.missing_module'",
    ):
        load_worker_handler("tests.fixtures.missing_module:ValidWorkerHandler")


def test_rejects_missing_worker_handler_class() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler class 'MissingWorkerHandler' was not found",
    ):
        load_worker_handler(f"{FIXTURE_MODULE}:MissingWorkerHandler")


@pytest.mark.parametrize(
    "attribute_name",
    [
        "worker_handler_factory",
        "not_a_worker_handler_class",
    ],
)
def test_rejects_imported_attribute_that_is_not_class(
    attribute_name: str,
) -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler import path must point to a WorkerHandler class",
    ):
        load_worker_handler(f"{FIXTURE_MODULE}:{attribute_name}")


def test_rejects_class_that_does_not_inherit_from_worker_handler() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler class must inherit from WorkerHandler",
    ):
        load_worker_handler(f"{FIXTURE_MODULE}:PlainClass")


def test_rejects_abstract_worker_handler_class() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler class must implement all abstract methods",
    ):
        load_worker_handler(f"{FIXTURE_MODULE}:AbstractWorkerHandler")


def test_rejects_base_worker_handler_class() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler class must implement all abstract methods",
    ):
        load_worker_handler(
            "mdds_worker_runtime_common.execution.handler:WorkerHandler"
        )


def test_rejects_handler_with_required_constructor_argument() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler class must have a no-argument constructor",
    ):
        load_worker_handler(f"{FIXTURE_MODULE}:ConstructorArgumentWorkerHandler")


def test_wraps_worker_handler_constructor_failure() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Cannot instantiate handler class while loading",
    ) as raised:
        load_worker_handler(f"{FIXTURE_MODULE}:ConstructorRaisesWorkerHandler")

    assert isinstance(raised.value.__cause__, RuntimeError)
    assert str(raised.value.__cause__) == "constructor failed"


def test_rejects_non_callable_execute_method() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler class must define callable execute",
    ):
        load_worker_handler(f"{FIXTURE_MODULE}:ExecuteNoneWorkerHandler")


def test_rejects_execute_method_without_context_parameter() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler method execute must have signature",
    ):
        load_worker_handler(f"{FIXTURE_MODULE}:ExecuteWithoutContextWorkerHandler")


def test_rejects_execute_method_with_keyword_only_context_parameter() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler method execute context parameter must be positional",
    ):
        load_worker_handler(
            f"{FIXTURE_MODULE}:ExecuteWithKeywordOnlyContextWorkerHandler"
        )


def test_rejects_execute_method_with_wrong_context_annotation() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match=(
            "Handler method execute context parameter must be annotated "
            "as WorkerExecutionContext or left unannotated"
        ),
    ):
        load_worker_handler(
            f"{FIXTURE_MODULE}:ExecuteWithWrongContextAnnotationWorkerHandler"
        )


def test_rejects_execute_method_with_wrong_return_annotation() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler method execute return type must be None",
    ):
        load_worker_handler(
            f"{FIXTURE_MODULE}:ExecuteWithWrongReturnAnnotationWorkerHandler"
        )


@pytest.mark.parametrize(
    "handler_class_name",
    [
        "AsyncExecuteWorkerHandler",
        "AsyncGeneratorExecuteWorkerHandler",
    ],
)
def test_rejects_asynchronous_execute_method(handler_class_name: str) -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler method execute must be synchronous",
    ):
        load_worker_handler(f"{FIXTURE_MODULE}:{handler_class_name}")


def test_loads_handler_with_varargs_constructor() -> None:
    handler = load_worker_handler(f"{FIXTURE_MODULE}:VarArgsConstructorWorkerHandler")

    assert isinstance(handler, VarArgsConstructorWorkerHandler)
    assert isinstance(handler, WorkerHandler)


def test_loads_handler_with_optional_constructor_argument() -> None:
    handler = load_worker_handler(
        f"{FIXTURE_MODULE}:OptionalConstructorArgumentWorkerHandler"
    )

    assert isinstance(handler, OptionalConstructorArgumentWorkerHandler)
    assert handler.value == "default"
    assert isinstance(handler, WorkerHandler)


def test_rejects_handler_with_invalid_constructor_signature() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Cannot inspect handler constructor",
    ):
        load_worker_handler(
            f"{FIXTURE_MODULE}:InvalidConstructorSignatureWorkerHandler"
        )


def test_rejects_handler_constructor_returning_non_handler_instance() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler constructor must return a WorkerHandler instance",
    ):
        load_worker_handler(
            f"{FIXTURE_MODULE}:"
            "ConstructorReturnsNonWorkerHandlerInstanceWorkerHandler"
        )


def test_loads_handler_with_postponed_annotations() -> None:
    handler = load_worker_handler(
        f"{POSTPONED_ANNOTATIONS_FIXTURE_MODULE}:" "PostponedAnnotationsWorkerHandler"
    )

    assert isinstance(handler, WorkerHandler)
    assert handler.__class__.__name__ == "PostponedAnnotationsWorkerHandler"


def test_rejects_handler_with_postponed_wrong_context_annotation() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler method execute context parameter must be annotated",
    ):
        load_worker_handler(
            f"{POSTPONED_ANNOTATIONS_FIXTURE_MODULE}:"
            "PostponedWrongContextAnnotationWorkerHandler"
        )


def test_rejects_handler_with_postponed_wrong_return_annotation() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Handler method execute return type must be None",
    ):
        load_worker_handler(
            f"{POSTPONED_ANNOTATIONS_FIXTURE_MODULE}:"
            "PostponedWrongReturnAnnotationWorkerHandler"
        )


def test_rejects_handler_with_unresolvable_postponed_annotation() -> None:
    with pytest.raises(
        WorkerHandlerLoadError,
        match="Cannot inspect handler method annotations",
    ):
        load_worker_handler(
            f"{POSTPONED_ANNOTATIONS_FIXTURE_MODULE}:"
            "PostponedUnresolvableContextAnnotationWorkerHandler"
        )

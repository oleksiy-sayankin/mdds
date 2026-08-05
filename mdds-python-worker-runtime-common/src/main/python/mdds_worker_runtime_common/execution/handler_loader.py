# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import importlib
import inspect
from types import ModuleType
from typing import Any

from mdds_worker_runtime_common.execution.context import WorkerExecutionContext
from mdds_worker_runtime_common.execution.handler import WorkerHandler


class WorkerHandlerLoadError(RuntimeError):
    """Raised when a configured WorkerHandler cannot be loaded or validated."""


def load_worker_handler(import_path: str) -> WorkerHandler:
    """Load, validate, and instantiate the configured WorkerHandler.

    The import path must have the following format:

        package.module:HandlerClass

    The imported attribute must be a concrete class inheriting from WorkerHandler.
    The class must have a no-argument constructor.
    """
    module_name, class_name = _parse_import_path(import_path)
    module = _import_module(module_name, import_path)
    handler_class = _get_handler_class(module, class_name, import_path)

    _validate_handler_class(handler_class, import_path)
    _validate_handler_method_signature(
        handler_class,
        "execute",
        import_path,
    )
    _validate_no_argument_constructor(handler_class, import_path)

    try:
        handler = handler_class()
    except Exception as exc:
        raise WorkerHandlerLoadError(
            f"Cannot instantiate handler class while loading '{import_path}'."
        ) from exc

    if not isinstance(handler, WorkerHandler):
        raise WorkerHandlerLoadError(
            "Handler constructor must return a WorkerHandler instance: "
            f"{import_path}"
        )

    return handler


def _parse_import_path(import_path: str) -> tuple[str, str]:
    if import_path is None or import_path.strip() == "":
        raise ValueError("handler import path cannot be null or blank.")

    normalized_import_path = import_path.strip()
    module_name, separator, class_name = normalized_import_path.partition(":")

    if separator == "":
        raise WorkerHandlerLoadError(
            "Handler import path must have format '<module>:<class>'."
        )

    if module_name.strip() == "":
        raise WorkerHandlerLoadError("Handler module name cannot be blank.")

    if class_name.strip() == "":
        raise WorkerHandlerLoadError("Handler class name cannot be blank.")

    if "." in class_name:
        raise WorkerHandlerLoadError(
            "Nested handler class names are not supported. "
            "Handler import path must have format '<module>:<class>'."
        )

    return module_name.strip(), class_name.strip()


def _import_module(module_name: str, import_path: str) -> ModuleType:
    try:
        return importlib.import_module(module_name)
    except ImportError as exc:
        raise WorkerHandlerLoadError(
            f"Cannot import handler module '{module_name}' "
            f"while loading '{import_path}'."
        ) from exc


def _get_handler_class(
    module: ModuleType,
    class_name: str,
    import_path: str,
) -> type[WorkerHandler]:
    try:
        imported_attribute = getattr(module, class_name)
    except AttributeError as exc:
        raise WorkerHandlerLoadError(
            f"Handler class '{class_name}' was not found "
            f"while loading '{import_path}'."
        ) from exc

    if not isinstance(imported_attribute, type):
        raise WorkerHandlerLoadError(
            f"Handler import path must point to a WorkerHandler class: {import_path}"
        )

    return imported_attribute


def _validate_handler_class(
    handler_class: type[Any],
    import_path: str,
) -> None:
    if not issubclass(handler_class, WorkerHandler):
        raise WorkerHandlerLoadError(
            f"Handler class must inherit from WorkerHandler: {import_path}"
        )

    if inspect.isabstract(handler_class):
        raise WorkerHandlerLoadError(
            f"Handler class must implement all abstract methods: {import_path}"
        )


def _signature_with_resolved_annotations(
    method,
    import_path: str,
) -> inspect.Signature:
    try:
        return inspect.signature(method, eval_str=True)
    except (NameError, TypeError, ValueError) as exc:
        raise WorkerHandlerLoadError(
            f"Cannot inspect handler method annotations: {import_path}"
        ) from exc


def _validate_handler_method_signature(
    handler_class: type[WorkerHandler],
    method_name: str,
    import_path: str,
) -> None:
    method = getattr(handler_class, method_name, None)

    if method is None or not callable(method):
        raise WorkerHandlerLoadError(
            f"Handler class must define callable {method_name}"
            f"(self, context): {import_path}"
        )

    if inspect.iscoroutinefunction(method) or inspect.isasyncgenfunction(method):
        raise WorkerHandlerLoadError(
            f"Handler method {method_name} must be synchronous: {import_path}"
        )

    signature = _signature_with_resolved_annotations(
        method,
        import_path,
    )
    parameters = list(signature.parameters.values())

    if len(parameters) != 2:
        raise WorkerHandlerLoadError(
            f"Handler method {method_name} must have signature "
            f"{method_name}(self, context): {import_path}"
        )

    context_parameter = parameters[1]

    if context_parameter.kind not in {
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    }:
        raise WorkerHandlerLoadError(
            f"Handler method {method_name} context parameter must be positional: "
            f"{import_path}"
        )

    annotation = context_parameter.annotation
    if annotation not in {inspect.Signature.empty, WorkerExecutionContext}:
        raise WorkerHandlerLoadError(
            f"Handler method {method_name} context parameter must be annotated "
            f"as WorkerExecutionContext or left unannotated: {import_path}"
        )

    return_annotation = signature.return_annotation
    if return_annotation not in {inspect.Signature.empty, None}:
        raise WorkerHandlerLoadError(
            f"Handler method {method_name} return type must be None "
            f"or left unannotated: {import_path}"
        )


def _validate_no_argument_constructor(
    handler_class: type[WorkerHandler],
    import_path: str,
) -> None:
    try:
        signature = inspect.signature(handler_class)
    except (TypeError, ValueError) as exc:
        raise WorkerHandlerLoadError(
            f"Cannot inspect handler constructor: {import_path}"
        ) from exc

    for parameter in signature.parameters.values():
        if parameter.kind in {
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        }:
            continue

        if parameter.default is inspect.Parameter.empty:
            raise WorkerHandlerLoadError(
                f"Handler class must have a no-argument constructor: {import_path}"
            )

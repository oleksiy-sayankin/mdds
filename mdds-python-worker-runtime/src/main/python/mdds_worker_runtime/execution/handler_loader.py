# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from __future__ import annotations

import importlib
import inspect
from types import ModuleType
from typing import Any

from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.handler import JobHandler


class JobHandlerLoadError(RuntimeError):
    """Raised when a configured job handler cannot be loaded or validated."""


class JobHandlerLoader:
    """Loads configured concrete JobHandler instances.

    The import path must have the following format:

        package.module:HandlerClass

    The imported attribute must be a concrete class inheriting from JobHandler.
    The class must have a no-argument constructor.

    The loader validates the configured handler class during initialization.
    Each load() call returns a fresh handler instance.
    """

    def __init__(self, import_path: str) -> None:
        module_name, class_name = self._parse_import_path(import_path)

        module = self._import_module(module_name, import_path)
        handler_class = self._get_handler_class(module, class_name, import_path)

        self._validate_handler_class(handler_class, import_path)
        self._validate_handler_method_signature(
            handler_class,
            "validate",
            import_path,
        )
        self._validate_handler_method_signature(
            handler_class,
            "execute",
            import_path,
        )
        self._validate_no_argument_constructor(handler_class, import_path)

        self._import_path = import_path.strip()
        self._handler_class = handler_class

    def load(self) -> JobHandler:
        """Create a fresh configured JobHandler instance."""
        try:
            return self._handler_class()
        except Exception as exc:
            raise JobHandlerLoadError(
                f"Cannot instantiate handler class while loading "
                f"'{self._import_path}'."
            ) from exc

    @staticmethod
    def _parse_import_path(import_path: str) -> tuple[str, str]:
        if import_path is None or import_path.strip() == "":
            raise ValueError("handler import path cannot be null or blank.")

        normalized_import_path = import_path.strip()
        module_name, separator, class_name = normalized_import_path.partition(":")

        if separator == "":
            raise JobHandlerLoadError(
                "Handler import path must have format '<module>:<class>'."
            )

        if module_name.strip() == "":
            raise JobHandlerLoadError("Handler module name cannot be blank.")

        if class_name.strip() == "":
            raise JobHandlerLoadError("Handler class name cannot be blank.")

        if "." in class_name:
            raise JobHandlerLoadError(
                "Nested handler class names are not supported. "
                "Handler import path must have format '<module>:<class>'."
            )

        return module_name.strip(), class_name.strip()

    @staticmethod
    def _import_module(module_name: str, import_path: str) -> ModuleType:
        try:
            return importlib.import_module(module_name)
        except ImportError as exc:
            raise JobHandlerLoadError(
                f"Cannot import handler module '{module_name}' "
                f"while loading '{import_path}'."
            ) from exc

    @staticmethod
    def _get_handler_class(
        module: ModuleType,
        class_name: str,
        import_path: str,
    ) -> type[JobHandler]:
        try:
            imported_attribute = getattr(module, class_name)
        except AttributeError as exc:
            raise JobHandlerLoadError(
                f"Handler class '{class_name}' was not found "
                f"while loading '{import_path}'."
            ) from exc

        if not isinstance(imported_attribute, type):
            raise JobHandlerLoadError(
                f"Handler import path must point to a JobHandler class: {import_path}"
            )

        return imported_attribute

    @staticmethod
    def _validate_handler_class(
        handler_class: type[Any],
        import_path: str,
    ) -> None:
        if not issubclass(handler_class, JobHandler):
            raise JobHandlerLoadError(
                f"Handler class must inherit from JobHandler: {import_path}"
            )

        if inspect.isabstract(handler_class):
            raise JobHandlerLoadError(
                f"Handler class must implement all abstract methods: {import_path}"
            )

    @staticmethod
    def _validate_handler_method_signature(
        handler_class: type[JobHandler],
        method_name: str,
        import_path: str,
    ) -> None:
        method = getattr(handler_class, method_name, None)

        if method is None or not callable(method):
            raise JobHandlerLoadError(
                f"Handler class must define callable {method_name}"
                f"(self, context): {import_path}"
            )

        signature = inspect.signature(method)
        parameters = list(signature.parameters.values())

        if len(parameters) != 2:
            raise JobHandlerLoadError(
                f"Handler method {method_name} must have signature "
                f"{method_name}(self, context): {import_path}"
            )

        context_parameter = parameters[1]

        if context_parameter.kind not in {
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        }:
            raise JobHandlerLoadError(
                f"Handler method {method_name} context parameter must be positional: "
                f"{import_path}"
            )

        annotation = context_parameter.annotation
        if annotation not in {inspect.Signature.empty, JobExecutionContext}:
            raise JobHandlerLoadError(
                f"Handler method {method_name} context parameter must be annotated "
                f"as JobExecutionContext or left unannotated: {import_path}"
            )

        return_annotation = signature.return_annotation
        if return_annotation not in {inspect.Signature.empty, None}:
            raise JobHandlerLoadError(
                f"Handler method {method_name} return type must be None "
                f"or left unannotated: {import_path}"
            )

    @staticmethod
    def _validate_no_argument_constructor(
        handler_class: type[JobHandler],
        import_path: str,
    ) -> None:
        try:
            signature = inspect.signature(handler_class)
        except (TypeError, ValueError) as exc:
            raise JobHandlerLoadError(
                f"Cannot inspect handler constructor: {import_path}"
            ) from exc

        for parameter in signature.parameters.values():
            if parameter.kind in {
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            }:
                continue

            if parameter.default is inspect.Parameter.empty:
                raise JobHandlerLoadError(
                    f"Handler class must have a no-argument constructor: "
                    f"{import_path}"
                )

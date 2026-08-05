# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from mdds_worker_runtime_common.execution.context import WorkerExecutionContext
from mdds_worker_runtime_common.execution.handler import WorkerHandler


class ValidWorkerHandler(WorkerHandler):
    def execute(self, context: WorkerExecutionContext) -> None:
        pass


class TwoNumbersSumWorkerHandler(WorkerHandler):
    def execute(self, context: WorkerExecutionContext) -> None:
        number_a = _parse_int(context.inputs.read("number_a"))
        number_b = _parse_int(context.inputs.read("number_b"))

        result = number_a + number_b

        context.outputs.write("sum", str(result).encode("utf-8"))


def _parse_int(value: bytes) -> int:
    return int(value.decode("utf-8").strip())


class ValidUnannotatedWorkerHandler(WorkerHandler):
    def execute(self, context) -> None:
        pass


class ValidPositionalOnlyContextWorkerHandler(WorkerHandler):
    def execute(self, context: WorkerExecutionContext, /) -> None:
        pass


class PlainClass:
    def execute(self, context: WorkerExecutionContext) -> None:
        pass


class AbstractWorkerHandler(WorkerHandler):
    pass


class ConstructorArgumentWorkerHandler(WorkerHandler):
    def __init__(self, value: str) -> None:
        self.value = value

    def execute(self, context: WorkerExecutionContext) -> None:
        pass


class ConstructorRaisesWorkerHandler(WorkerHandler):
    def __init__(self) -> None:
        raise RuntimeError("constructor failed")

    def execute(self, context: WorkerExecutionContext) -> None:
        pass


class ExecuteWithoutContextWorkerHandler(WorkerHandler):
    def execute(self) -> None:
        pass


class ExecuteWithKeywordOnlyContextWorkerHandler(WorkerHandler):
    def execute(self, *, context: WorkerExecutionContext) -> None:
        pass


class ExecuteWithWrongContextAnnotationWorkerHandler(WorkerHandler):
    def execute(self, context: str) -> None:
        pass


class ExecuteWithWrongReturnAnnotationWorkerHandler(WorkerHandler):
    def execute(self, context: WorkerExecutionContext) -> str:
        return "invalid"


class AsyncExecuteWorkerHandler(WorkerHandler):
    async def execute(self, context: WorkerExecutionContext) -> None:
        pass


class AsyncGeneratorExecuteWorkerHandler(WorkerHandler):
    async def execute(self, context: WorkerExecutionContext) -> None:
        yield


def worker_handler_factory() -> ValidWorkerHandler:
    return ValidWorkerHandler()


not_a_worker_handler_class = ValidWorkerHandler()


class ExecuteNoneWorkerHandler(WorkerHandler):
    execute = None


class InvalidConstructorSignatureWorkerHandler(WorkerHandler):
    __signature__ = "invalid"

    def execute(self, context: WorkerExecutionContext) -> None:
        pass


class VarArgsConstructorWorkerHandler(WorkerHandler):
    def __init__(self, *args, **kwargs) -> None:
        pass

    def execute(self, context: WorkerExecutionContext) -> None:
        pass


class OptionalConstructorArgumentWorkerHandler(WorkerHandler):
    def __init__(self, value: str = "default") -> None:
        self.value = value

    def execute(self, context: WorkerExecutionContext) -> None:
        pass


class ConstructorReturnsNonWorkerHandlerInstanceWorkerHandler(WorkerHandler):
    """Handler whose constructor returns an object of an invalid type."""

    def __new__(cls):
        return object()

    def execute(self, context: WorkerExecutionContext) -> None:
        pass

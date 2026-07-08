# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.handler import JobHandler


class ValidJobHandler(JobHandler):
    def execute(self, context: JobExecutionContext) -> None:
        pass


class ValidUnannotatedJobHandler(JobHandler):
    def execute(self, context) -> None:
        pass


class PlainClass:
    def execute(self, context: JobExecutionContext) -> None:
        pass


class AbstractJobHandler(JobHandler):
    pass


class ConstructorArgumentJobHandler(JobHandler):
    def __init__(self, value: str) -> None:
        self.value = value

    def execute(self, context: JobExecutionContext) -> None:
        pass


class ConstructorRaisesJobHandler(JobHandler):
    def __init__(self) -> None:
        raise RuntimeError("constructor failed")

    def execute(self, context: JobExecutionContext) -> None:
        pass


class ExecuteWithoutContextJobHandler(JobHandler):
    def execute(self) -> None:
        pass


class ExecuteWithKeywordOnlyContextJobHandler(JobHandler):
    def execute(self, *, context: JobExecutionContext) -> None:
        pass


class ExecuteWithWrongContextAnnotationJobHandler(JobHandler):
    def execute(self, context: str) -> None:
        pass


class ExecuteWithWrongReturnAnnotationJobHandler(JobHandler):
    def execute(self, context: JobExecutionContext) -> str:
        return "invalid"


def handler_factory() -> ValidJobHandler:
    return ValidJobHandler()


not_a_handler_class = ValidJobHandler()


class ExecuteNoneJobHandler(JobHandler):

    execute = None


class InvalidConstructorSignatureJobHandler(JobHandler):
    __signature__ = "invalid"

    def execute(self, context: JobExecutionContext) -> None:
        pass


class VarArgsConstructorJobHandler(JobHandler):
    def __init__(self, *args, **kwargs) -> None:
        pass

    def execute(self, context: JobExecutionContext) -> None:
        pass


class OptionalConstructorArgumentJobHandler(JobHandler):
    def __init__(self, value: str = "default") -> None:
        self.value = value

    def execute(self, context: JobExecutionContext) -> None:
        pass


class WritingExecuteJobHandler(JobHandler):

    def execute(self, context: JobExecutionContext) -> None:
        context.outputs.write("solution", b"execution-result")


class FailingExecuteJobHandler(JobHandler):

    def execute(self, context: JobExecutionContext) -> None:
        raise RuntimeError("execute failed")


class TwoNumbersSumJobHandler(JobHandler):
    def execute(self, context: JobExecutionContext) -> None:
        number_a = _parse_int(context.inputs.read("number_a"))
        number_b = _parse_int(context.inputs.read("number_b"))

        result = number_a + number_b

        context.outputs.write("sum", str(result).encode("utf-8"))


def _parse_int(value: bytes) -> int:
    return int(value.decode("utf-8").strip())


class UnexpectedExecutionErrorJobHandler(JobHandler):
    def execute(self, context: JobExecutionContext) -> None:
        raise RuntimeError("Unexpected execution error. Test message.")


class ConstructorReturnsNonJobHandlerInstanceJobHandler(JobHandler):
    """Handler whose constructor returns an object that is not a JobHandler."""

    def __new__(cls):
        return object()

    def execute(self, context: JobExecutionContext) -> None:
        """Execute job-specific logic."""

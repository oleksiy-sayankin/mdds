# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

from mdds_worker_runtime.execution.context import JobExecutionContext
from mdds_worker_runtime.execution.handler import JobHandler


class ValidJobHandler(JobHandler):
    def validate(self, context: JobExecutionContext) -> None:
        pass

    def execute(self, context: JobExecutionContext) -> None:
        pass


class ValidUnannotatedJobHandler(JobHandler):
    def validate(self, context) -> None:
        pass

    def execute(self, context) -> None:
        pass


class PlainClass:
    def validate(self, context: JobExecutionContext) -> None:
        pass

    def execute(self, context: JobExecutionContext) -> None:
        pass


class AbstractJobHandler(JobHandler):
    def validate(self, context: JobExecutionContext) -> None:
        pass


class ConstructorArgumentJobHandler(JobHandler):
    def __init__(self, value: str) -> None:
        self.value = value

    def validate(self, context: JobExecutionContext) -> None:
        pass

    def execute(self, context: JobExecutionContext) -> None:
        pass


class ConstructorRaisesJobHandler(JobHandler):
    def __init__(self) -> None:
        raise RuntimeError("constructor failed")

    def validate(self, context: JobExecutionContext) -> None:
        pass

    def execute(self, context: JobExecutionContext) -> None:
        pass


class ValidateWithoutContextJobHandler(JobHandler):
    def validate(self) -> None:
        pass

    def execute(self, context: JobExecutionContext) -> None:
        pass


class ExecuteWithoutContextJobHandler(JobHandler):
    def validate(self, context: JobExecutionContext) -> None:
        pass

    def execute(self) -> None:
        pass


class ValidateWithKeywordOnlyContextJobHandler(JobHandler):
    def validate(self, *, context: JobExecutionContext) -> None:
        pass

    def execute(self, context: JobExecutionContext) -> None:
        pass


class ExecuteWithKeywordOnlyContextJobHandler(JobHandler):
    def validate(self, context: JobExecutionContext) -> None:
        pass

    def execute(self, *, context: JobExecutionContext) -> None:
        pass


class ValidateWithWrongContextAnnotationJobHandler(JobHandler):
    def validate(self, context: str) -> None:
        pass

    def execute(self, context: JobExecutionContext) -> None:
        pass


class ExecuteWithWrongContextAnnotationJobHandler(JobHandler):
    def validate(self, context: JobExecutionContext) -> None:
        pass

    def execute(self, context: str) -> None:
        pass


class ValidateWithWrongReturnAnnotationJobHandler(JobHandler):
    def validate(self, context: JobExecutionContext) -> str:
        return "invalid"

    def execute(self, context: JobExecutionContext) -> None:
        pass


class ExecuteWithWrongReturnAnnotationJobHandler(JobHandler):
    def validate(self, context: JobExecutionContext) -> None:
        pass

    def execute(self, context: JobExecutionContext) -> str:
        return "invalid"


def handler_factory() -> ValidJobHandler:
    return ValidJobHandler()


not_a_handler_class = ValidJobHandler()


class ValidateNoneJobHandler(JobHandler):
    validate = None

    def execute(self, context: JobExecutionContext) -> None:
        pass


class ExecuteNoneJobHandler(JobHandler):
    def validate(self, context: JobExecutionContext) -> None:
        pass

    execute = None


class InvalidConstructorSignatureJobHandler(JobHandler):
    __signature__ = "invalid"

    def validate(self, context: JobExecutionContext) -> None:
        pass

    def execute(self, context: JobExecutionContext) -> None:
        pass


class VarArgsConstructorJobHandler(JobHandler):
    def __init__(self, *args, **kwargs) -> None:
        pass

    def validate(self, context: JobExecutionContext) -> None:
        pass

    def execute(self, context: JobExecutionContext) -> None:
        pass


class OptionalConstructorArgumentJobHandler(JobHandler):
    def __init__(self, value: str = "default") -> None:
        self.value = value

    def validate(self, context: JobExecutionContext) -> None:
        pass

    def execute(self, context: JobExecutionContext) -> None:
        pass

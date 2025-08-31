# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""
Executor service is RabbitMQ consumer + solver. It takes Task from Task Queue, solves SLAE and puts
result (if it exists) into Result Queue.
"""
from datetime import datetime, UTC
import logging
import os
import numpy as np
import pika
from pika.spec import PERSISTENT_DELIVERY_MODE

from mdds_dto.result_dto import ResultDTO
from mdds_dto.task_dto import TaskDTO
from mdds_dto.task_status import TaskStatus
from mdds_utility.rabbitmq_helper import (
    connect_to_rabbit_mq,
    close_rabbit_mq_connection,
    declare_queues,
)
from slae_solver.solvers.numpy_exact_solver import NumpyExactSolver
from slae_solver.solvers.numpy_lstsq_solver import NumpyLstsqSolver
from slae_solver.solvers.numpy_pinv_solver import NumpyPinvSolver
from slae_solver.solvers.petsc_solver import PetscSolver
from slae_solver.solvers.scipy_gmres_solver import ScipyGmresSolver

from common_logging.setup_logging import setup_logging

# Apply logging config
setup_logging()

logger = logging.getLogger(__name__)

# Configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
TASK_QUEUE_NAME = "task_queue"
RESULT_QUEUE_NAME = "result_queue"

# Solver mapping
SOLVER_MAPPING = {
    "numpy_exact_solver": NumpyExactSolver,
    "numpy_lstsq_solver": NumpyLstsqSolver,
    "numpy_pinv_solver": NumpyPinvSolver,
    "petsc_solver": PetscSolver,
    "scipy_gmres_solver": ScipyGmresSolver,
}


def solve_slae(
    matrix: list, rhs: list, slae_solving_method: str
) -> tuple[list | None, TaskStatus, str | None]:
    if slae_solving_method not in SOLVER_MAPPING:
        logger.error(f"Unknown solver method requested: {slae_solving_method}")
        return None, TaskStatus.ERROR, f"Unknown solver method: {slae_solving_method}"

    # Load files into numpy arrays
    matrix = np.array(matrix)  # We expect N x N matrix here
    rhs = np.array(rhs).reshape(
        -1
    )  # Here we convert any shape of vector (horizontal or vertical) to the shape [1, 2, 3, 4,...]
    logger.info(f"Loaded matrix A shape: {matrix.shape}, vector b shape: {rhs.shape}")

    # Instantiate solver
    solver_class = SOLVER_MAPPING[slae_solving_method]
    solver = solver_class()

    # Solve SLAE
    try:
        solution = solver.solve(matrix, rhs)
        logger.info(f"Solved SLAE successfully using {slae_solving_method}")
        return solution.tolist(), TaskStatus.DONE, None
    except Exception as e:
        logger.exception("Error while solving SLAE")
        return None, TaskStatus.ERROR, f"error: {str(e)}"


def callback(channel, method, properties, body):
    try:
        if logger.isEnabledFor(logging.DEBUG):
            body_str = body if isinstance(body, str) else str(body)
            max_len = 200
            logger.debug(
                "Body preview: %s%s",
                body_str[:max_len],
                "..." if len(body_str) > max_len else "",
            )

        task = TaskDTO.model_validate_json(body)
        logger.info(
            f"Executor got task {task.task_id} with method={task.slae_solving_method}"
        )

        solution, status, error_message = solve_slae(
            task.matrix, task.rhs, task.slae_solving_method
        )

        result = ResultDTO(
            task_id=task.task_id,
            date_time_task_created=task.date_time_created,
            date_time_task_finished=datetime.now(UTC),
            status=status,
            solution=solution if status == TaskStatus.DONE else None,
            error_message=error_message if status == TaskStatus.ERROR else None,
        )

        # Publish solution to result queue
        channel.basic_publish(
            exchange="",
            routing_key=RESULT_QUEUE_NAME,
            body=result.model_dump_json(),
            properties=pika.BasicProperties(delivery_mode=PERSISTENT_DELIVERY_MODE),
        )
        logger.info(f"Executor finished task {task.task_id}, status={status}")
        channel.basic_ack(delivery_tag=method.delivery_tag)  # mark message as processed

    except Exception:
        logger.exception("Failed to process task, rejecting message")
        # reject message and don't requeue
        try:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            # TODO: Think about putting this to Dead Letter Queue in the future.
        except Exception:
            logger.exception("Failed to nack message")


def main():
    rabbitmq_connection = None
    rabbitmq_channel = None
    try:
        rabbitmq_connection, rabbitmq_channel = connect_to_rabbit_mq(RABBITMQ_HOST)
        declare_queues(rabbitmq_channel, TASK_QUEUE_NAME, RESULT_QUEUE_NAME)
        rabbitmq_channel.basic_qos(prefetch_count=1)
        rabbitmq_channel.basic_consume(
            queue=TASK_QUEUE_NAME, on_message_callback=callback
        )
        logger.info("Executor waiting for tasks...")
        rabbitmq_channel.start_consuming()

    except KeyboardInterrupt:
        logger.info("Executor shutting down by user request")
    except Exception:
        logger.exception("Unexpected error in executor")
    finally:
        close_rabbit_mq_connection(rabbitmq_connection, rabbitmq_channel)


if __name__ == "__main__":
    main()

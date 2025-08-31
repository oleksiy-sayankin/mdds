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
from slae_solver.solvers.numpy_exact_solver import NumpyExactSolver
from slae_solver.solvers.numpy_lstsq_solver import NumpyLstsqSolver
from slae_solver.solvers.numpy_pinv_solver import NumpyPinvSolver
from slae_solver.solvers.petsc_solver import PetscSolver
from slae_solver.solvers.scipy_gmres_solver import ScipyGmresSolver

from common_logging.setup_logging import setup_logging

# Apply logging config
setup_logging()

logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")

# Solver mapping
SOLVER_MAPPING = {
    "numpy_exact_solver": NumpyExactSolver,
    "numpy_lstsq_solver": NumpyLstsqSolver,
    "numpy_pinv_solver": NumpyPinvSolver,
    "petsc_solver": PetscSolver,
    "scipy_gmres_solver": ScipyGmresSolver,
}


def get_connection():
    # TODO:
    #  1. Think about reading heartbeat and blocked_connection_timeout from configuration file;
    #  2. What about re-connect strategies? Say, if there is no connection, wait for 1s, if there is no again, then
    #     5s and then 10s and so on. Think in future.
    #  3. Think about secure connection to RabbitMQ host (ssl or whatever).
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            RABBITMQ_HOST, heartbeat=600, blocked_connection_timeout=300
        )
    )


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


def callback(channel, delivery, properties, body):
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
            f"Executor got task {task["task_id"]} with method={task["slae_solving_method"]}"
        )

        solution, status, error_message = solve_slae(
            task["matrix"], task["rhs"], task["slae_solving_method"]
        )

        result = ResultDTO(
            task_id=task["task_id"],
            date_time_task_created=task["date_time_created"],
            date_time_task_finished=datetime.now(UTC),
            status=status,
            solution=solution if status == TaskStatus.DONE else None,
            error_message=error_message if status == TaskStatus.ERROR else None,
        )

        # Publish solution to result queue
        channel.basic_publish(
            exchange="",
            routing_key="result_queue",
            body=result.model_dump_json(),
            properties=pika.BasicProperties(delivery_mode=PERSISTENT_DELIVERY_MODE),
        )
        logger.info(f"Executor finished task {task["task_id"]}, status={status}")
        channel.basic_ack(
            delivery_tag=delivery.delivery_tag
        )  # mark message as processed

    except Exception:
        logger.exception("Failed to process task, rejecting message")
        # reject message and don't requeue
        try:
            channel.basic_nack(delivery_tag=delivery.delivery_tag, requeue=False)
            # TODO: Think about putting this to Dead Letter Queue in the future.
        except Exception:
            logger.exception("Failed to nack message")


def main():
    connection = None
    channel = None
    try:
        connection = get_connection()
        channel = connection.channel()
        channel.queue_declare(
            queue="task_queue", durable=True
        )  # declare persistent queue
        channel.queue_declare(queue="result_queue", durable=True)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue="task_queue", on_message_callback=callback)
        logger.info("Executor waiting for tasks...")

        channel.start_consuming()

    except KeyboardInterrupt:
        logger.info("Executor shutting down by user request")
    except Exception:
        logger.exception("Unexpected error in executor")
    finally:
        if channel is not None and channel.is_open:
            try:
                channel.close()
            except Exception:
                logger.exception("Error closing channel")
        if connection is not None and connection.is_open:
            try:
                connection.close()
            except Exception:
                logger.exception("Error closing connection")


if __name__ == "__main__":
    main()

# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""
Executor service is RabbitMQ consumer + solver. It takes Task from Task Queue, solves SLAE and puts
result (if it exists) into Result Queue.
"""

import json
import logging
import os
import numpy as np
import pika

from slae_solver.solvers.numpy_exact_solver import NumpyExactSolver
from slae_solver.solvers.numpy_lstsq_solver import NumpyLstsqSolver
from slae_solver.solvers.numpy_pinv_solver import NumpyPinvSolver
from slae_solver.solvers.petsc_solver import PetscSolver
from slae_solver.solvers.scipy_gmres_solver import ScipyGmresSolver

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")

# Solver mapping
SOLVER_MAPPING = {
    "numpy_exact_solver": NumpyExactSolver,
    "numpy_lstsq_solver": NumpyLstsqSolver,
    "numpy_pinv_solver": NumpyPinvSolver,
    "petsc_solver": PetscSolver,
    "scipy_gmres_solver": ScipyGmresSolver,
}

def get_connection():
    return pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))

def solve_slae(A, b, method):
    if method not in SOLVER_MAPPING:
        logging.error(f"Unknown solver method requested: {method}")
        return {"error": f"Unknown solver method: {method}"}

    # Load files into numpy arrays
    A = np.loadtxt(matrix.file, delimiter=",")
    b = np.loadtxt(rhs.file, delimiter=",")
    logging.info(f"Loaded matrix A shape: {A.shape}, vector b shape: {b.shape}")

    # Instantiate solver
    solver_class = SOLVER_MAPPING[method]
    solver = solver_class()

    # Solve SLAE
    try:
        x = solver.solve(A, b)
        logging.info(f"Solved SLAE successfully using {method}")
    except Exception as e:
        logging.exception("Error while solving SLAE")
        return {"error": str(e)}

def callback(ch, method, properties, body):
    task = json.loads(body)
    task_id = task["task_id"]
    logger.info(f"Executor got task {task_id} with method={task['method']}")

    solution, status = solve_slae(task["A"], task["b"], task["method"])

    result = {
        "task_id": task_id,
        "status": status,
        "solution": solution,
    }

    ch.basic_publish(exchange="", routing_key="result_queue", body=json.dumps(result).encode())
    logger.info(f"Executor finished task {task_id}, status={status}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    connection = get_connection()
    channel = connection.channel()
    channel.queue_declare(queue="task_queue")
    channel.queue_declare(queue="result_queue")

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="task_queue", on_message_callback=callback)
    logger.info("Executor waiting for tasks...")
    channel.start_consuming()

if __name__ == "__main__":
    main()

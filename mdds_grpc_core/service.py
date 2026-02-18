# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import logging
import multiprocessing as mp
import time

from google.protobuf.timestamp_pb2 import Timestamp
from datetime import datetime, timezone
from dictionary import ThreadSafeDictionary
from generated import solver_pb2, solver_pb2_grpc
from constants import (
    IN_PROGRESS,
    DONE,
    ERROR,
    CANCELLED,
    TERMINAL,
    DECLINED,
    COMPLETED,
    JOB_TIMEOUT,
)
from job import Job


# Mapping solver name -> Python class
SOLVER_MAP = {
    "numpy_exact_solver": "NumpyExactSolver",
    "numpy_lstsq_solver": "NumpyLstsqSolver",
    "numpy_pinv_solver": "NumpyPinvSolver",
    "petsc_solver": "PetscSolver",
    "scipy_gmres_solver": "ScipyGmresSolver",
}


logger = logging.getLogger(f"{__name__}.SolverService")


def ts_from_unix(sec: float) -> Timestamp:
    t = Timestamp()
    t.FromDatetime(datetime.fromtimestamp(sec, tz=timezone.utc))
    return t


def solve_job(matrix, rhs, method, conn):
    """
    Solves system of linear algebraic equations and sends result to connection

    :param matrix: matrix of coefficients (2D, dense or sparse) for SLAE
    :param rhs: right-hand side vector
    :param method: method to solve
    :param conn: connection to parent process
    :return: solution x of SLAE, job status and error message if any
    """
    try:
        # Load solver from module by method name
        module = __import__(
            f"slae_solver.solvers.{method}", fromlist=[SOLVER_MAP[method]]
        )
        solver_class = getattr(module, SOLVER_MAP[method])
        solver = solver_class()

        # Solve system of linear algebraic equations
        solution = solver.solve(matrix, rhs)

        # Send solution x to connection
        conn.send(
            (
                DONE,
                solution,
                "Solved system of linear algebraic equations",
            )
        )
    except Exception as e:
        conn.send((ERROR, [], f"{type(e).__name__}: {e}"))
    finally:
        conn.close()


def terminate_process(proc: mp.Process, timeout: float = 3.0) -> None:
    """Try graceful terminate first, then hard kill."""
    if proc is None:
        return
    if proc.is_alive():
        proc.terminate()  # SIGTERM
        proc.join(timeout=timeout)
    if proc.is_alive():
        proc.kill()  # SIGKILL
        proc.join(timeout=timeout)


ctx = mp.get_context("spawn")  # Create multi process context


def get_progress(job_status: int, start_time: float) -> int:
    if job_status == IN_PROGRESS:
        p = int((time.time() - start_time) / JOB_TIMEOUT * 100)
        return max(0, min(99, p))
    if job_status == DONE:
        return 100
    if job_status in (ERROR, CANCELLED):
        return 70
    return 0


class SolverService(solver_pb2_grpc.SolverServiceServicer):
    """
    gRPC Service that takes solve request (matrix of coefficients, right hand side and solve method),
    converts Protobuf data to Python data, solves system of linear algebraic equations and send the
    result as Protobuf data back to the caller.
    """

    def __init__(self, active: ThreadSafeDictionary):
        super().__init__()
        self.active = active

    def SubmitJob(self, request, context):
        job_id = request.jobId
        if not job_id:
            logger.warning("No job id provided")
            return solver_pb2.SubmitJobResponse(
                requestStatus=DECLINED,
                requestStatusDetails="Job id is invalid: empty or null",
            )

        if self.active.get(job_id):
            logger.warning(f"Job {job_id} is already submitted")
            return solver_pb2.SubmitJobResponse(
                jobId=job_id,
                requestStatus=DECLINED,
                requestStatusDetails="Job already submitted",
            )

        method = request.method
        if method not in SOLVER_MAP:
            logger.warning(f"Method {method} not supported")
            return solver_pb2.SubmitJobResponse(
                jobId=job_id,
                requestStatus=DECLINED,
                requestStatusDetails=f"Unknown method: {method}",
            )

        # Convert data
        matrix = [list(row.values) for row in request.matrix]
        rhs = list(request.rhs)

        parent_conn, child_conn = ctx.Pipe(duplex=False)
        # Start separate process to solve SLAE
        process = ctx.Process(target=solve_job, args=(matrix, rhs, method, child_conn))
        process.start()
        child_conn.close()

        # Save job to dictionary
        self.active.put(
            job_id,
            Job(
                process,
                IN_PROGRESS,
                [],
                "Job submitted and is in progress",
                time.time(),
                None,
                parent_conn,
            ),
        )

        logger.info(f"Job {job_id} submitted and is in progress")
        return solver_pb2.SubmitJobResponse(
            jobId=job_id,
            requestStatus=COMPLETED,
            requestStatusDetails=f"Successfully submitted job for job: {job_id}",
        )

    def CancelJob(self, request, context):
        job_id = request.jobId
        if not job_id:
            logger.warning("No job id provided")
            return solver_pb2.CancelJobResponse(
                requestStatus=DECLINED,
                requestStatusDetails="Job id is empty",
            )
        job = self.active.get(job_id, None)
        if job is None:
            logger.warning(f"Job {job_id} does not exist")
            return solver_pb2.CancelJobResponse(
                jobId=job_id,
                requestStatus=DECLINED,
                requestStatusDetails=f"Job {job_id} is not found. Total active jobs count: {self.active.size()}",
            )
        with job.lock:
            if job.jobStatus != IN_PROGRESS:
                logger.warning(
                    f"Job {job_id} is not in progress. Job status is {solver_pb2.JobStatus.Name(job.jobStatus)}"
                )
                return solver_pb2.CancelJobResponse(
                    jobId=job_id,
                    requestStatus=DECLINED,
                    requestStatusDetails=f"Job {job_id} is not in IN_PROGRESS state. Job status is {solver_pb2.JobStatus.Name(job.jobStatus)}",
                )
            # mark that process is terminated intentionally and not by error
            job.jobStatus = CANCELLED
            job.jobMessage = "Cancelled by request"
            job.endTime = time.time()

        terminate_process(job.process)
        logger.info(f"Job {job_id} is cancelled")
        return solver_pb2.CancelJobResponse(
            jobId=job_id,
            requestStatus=COMPLETED,
            requestStatusDetails=f"Job {job_id} is cancelled",
        )

    def GetJobStatus(self, request, context):
        job_id = request.jobId
        if not job_id:
            logger.warning("No job id provided")
            return solver_pb2.GetJobStatusResponse(
                requestStatus=DECLINED,
                requestStatusDetails="Job id is empty or null",
            )
        job = self.active.get(job_id, None)
        if job is None:
            logger.warning(f"Job {job_id} does not exist")
            return solver_pb2.GetJobStatusResponse(
                jobId=job_id,
                requestStatus=DECLINED,
                requestStatusDetails=f"Job {job_id} is not found. Total active jobs count: {self.active.size()}",
            )
        with job.lock:
            job_status = job.jobStatus
            solution = job.solution
            job_message = job.jobMessage
            start_time = job.startTime
            end_time = job.endTime or time.time()
            if job_status in TERMINAL:
                job.delivered = True

        progress = get_progress(job_status, start_time)

        logger.debug(
            f"Found job {job_id} with status: {solver_pb2.JobStatus.Name(job_status)}"
        )
        return solver_pb2.GetJobStatusResponse(
            requestStatus=COMPLETED,
            requestStatusDetails="Found job status",
            jobId=job_id,
            startTime=ts_from_unix(start_time),
            endTime=ts_from_unix(end_time),
            progress=progress,
            jobStatus=job_status,
            solution=solution,
            jobMessage=job_message,
        )

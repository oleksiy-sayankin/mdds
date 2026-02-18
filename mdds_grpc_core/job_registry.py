# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import logging
import threading
import time

from threading import Event
from constants import IN_PROGRESS, ERROR, TERMINAL, JOB_TIMEOUT, RESULT_TIME_TO_LIVE
from dictionary import ThreadSafeDictionary
from job import Job

logger = logging.getLogger(f"{__name__}.JobRegistry")


def finalize_job(job: Job):
    """Here we close job connection (used to get results from Pipe) and finalize job process with `join`"""
    try:
        job.connection.close()
    except Exception:
        pass
    try:
        job.process.join(timeout=0.1)
    except Exception:
        pass


def terminate_job(job: Job):
    """Here we close job connection and finalize job process with `kill`"""
    try:
        job.connection.close()
    except Exception:
        pass
    proc = job.process
    if proc is None:
        return
    if proc.is_alive():
        proc.terminate()  # SIGTERM
        proc.join(timeout=1)
    if proc.is_alive():
        proc.kill()  # SIGKILL
        proc.join(timeout=1)


def terminate_all_jobs(
    active: ThreadSafeDictionary,
):
    """Terminate all active jobs by force and clear list of active jobs"""
    for job_id in active.keys():
        job = active.get(job_id)
        if job:
            terminate_job(job)
    active.clear()


def clean_delivered_job(
    active: ThreadSafeDictionary,
    stop_event: Event,
    poll_interval_seconds: float = 0.2,
):
    """
    Runs in a background thread to clean delivered jobs.
    """
    while not stop_event.is_set():
        for job_id in active.keys():
            job = active.get(job_id)

            if job:
                with job.lock:
                    delivered = job.delivered
                    status = job.jobStatus
                    process = job.process
                    start_time = job.startTime
                    end_time = job.endTime

                # If job result is delivered, and its status is terminal
                # statuses list, we can safely remove job item from the dictionary
                # and exit clean job daemon
                if delivered and status in TERMINAL and not process.is_alive():
                    active.pop(job_id, None)
                    finalize_job(job)
                    continue

                # skip infinite loops if client did not ask for result
                if (
                    status in TERMINAL
                    and end_time is not None
                    and time.time() - end_time > RESULT_TIME_TO_LIVE
                ):
                    active.pop(job_id, None)
                    finalize_job(job)
                    continue

                # kill job if we reach timeout and mar job as error
                if status == IN_PROGRESS and time.time() - start_time > JOB_TIMEOUT:
                    with job.lock:
                        job.jobStatus = ERROR
                        job.jobMessage = f"Timeout for job {job_id}"
                        job.endTime = time.time()
                    terminate_job(job)

        if stop_event.wait(poll_interval_seconds):
            logger.info("Job cleaner is stopped")
            break


def watch_job_result(
    active: ThreadSafeDictionary, stop_event: Event, poll_interval_seconds: float = 0.2
):
    """
    Runs in a background thread.
    Reads worker result from Pipe and updates Job in `active`.
    """
    while not stop_event.is_set():
        for job_id in active.keys():
            job = active.get(job_id)

            if job:
                try:
                    # We have data from process -> read and update Job
                    with job.lock:
                        last_status = job.jobStatus
                        connection = job.connection
                        process = job.process

                    if last_status == IN_PROGRESS and connection.poll(0):
                        status, solution, msg = connection.recv()
                        with job.lock:
                            job.jobStatus = status
                            job.solution = solution
                            job.jobMessage = msg
                            job.endTime = time.time()
                        continue

                    # Process is dead, but we have no results -> let's find out why
                    if not process.is_alive():
                        # If job is in progress but process is dead, mark it as error
                        if last_status == IN_PROGRESS:
                            with job.lock:
                                job.jobStatus = ERROR
                                job.jobMessage = (
                                    f"Worker exited, exitcode={process.exitcode}"
                                )
                                job.endTime = time.time()

                except Exception as e:
                    with job.lock:
                        # If job is in progress, but we raise an exception, mark it as error
                        if job.jobStatus == IN_PROGRESS:
                            job.jobStatus = ERROR
                            job.jobMessage = f"Watcher error: {type(e).__name__}: {e}"
                            job.endTime = time.time()

        if stop_event.wait(poll_interval_seconds):
            logger.info("Job result watcher is stopped")
            break


class JobRegistry:
    """
    Job registry that starts threads to watch over dictionary of active jobs
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(JobRegistry, cls).__new__(cls)

        return cls._instance

    def __init__(self):
        if getattr(self, "_initialized", False):
            return
        self._watcher = None
        self._cleaner = None
        self._stop_event = None
        self.active = ThreadSafeDictionary()
        self._started = False
        self._initialized = True
        logger.info("Initialized job registry")

    def start(self):
        if self._started:
            return

        self._stop_event = threading.Event()

        self.active.clear()
        self._watcher = threading.Thread(
            target=watch_job_result,
            args=(
                self.active,
                self._stop_event,
            ),
            daemon=False,
        )

        self._cleaner = threading.Thread(
            target=clean_delivered_job,
            args=(
                self.active,
                self._stop_event,
            ),
            daemon=False,
        )

        # Start watcher thread. In this watcher we periodically update `active` dictionary, getting
        # results (if any) from the solve process.
        self._watcher.start()

        # This cleaner watches if a job result was delivered and if so, removes job from dictionary
        self._cleaner.start()

        self._started = True
        logger.info("Started job registry")

    def stop(self):
        if self._started:
            self._stop_event.set()
            self._watcher.join(timeout=1)
            self._cleaner.join(timeout=1)
            self._started = False
            terminate_all_jobs(active=self.active)
            logger.info("Stopped job registry")

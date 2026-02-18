# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
import os
import logging
import grpc

from constants import MAX_MESSAGE_LENGTH
from service import SolverService
from generated import solver_pb2_grpc, solver_pb2
from concurrent import futures
from grpc_health.v1 import health, health_pb2_grpc, health_pb2
from grpc_reflection.v1alpha import reflection
from job_registry import JobRegistry

logger = logging.getLogger(f"{__name__}.GrpcServer")


class GrpcServer:
    """
    Singleton to create and strat gRPC Server.
    Creates single instance of gRPC Server, that we can register and start.
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        """
        Creates and returns single instance of gRPC Server.
        If instance exists, returns it. Otherwise, creates new instance.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        """
        Initializes gRPC Server, if not yet initialized.
        Setups gRPC host and port, creates gRPC server and adds insecure port.
        """
        if not hasattr(self, "initialized"):
            grpc_host = os.getenv("MDDS_EXECUTOR_GRPC_SERVER_HOST", "localhost")
            grpc_port = int(os.getenv("MDDS_EXECUTOR_GRPC_SERVER_PORT", 50051))
            self.SERVER_ADDRESS = f"{grpc_host}:{grpc_port}"
            self.server = grpc.server(
                futures.ThreadPoolExecutor(max_workers=10),
                options=[
                    ("grpc.max_send_message_length", MAX_MESSAGE_LENGTH),
                    ("grpc.max_receive_message_length", MAX_MESSAGE_LENGTH),
                ],
            )
            self.server.add_insecure_port(self.SERVER_ADDRESS)
            self.health_servicer = health.HealthServicer()
            self.job_registry = JobRegistry()
            self.initialized = True
            logger.info(f"Initialized gRPC server on {self.SERVER_ADDRESS}")

    def register(self) -> None:
        """
        Registers services in gRPC server.
        Register SolverService in gRPC server.
        """
        solver_pb2_grpc.add_SolverServiceServicer_to_server(
            SolverService(self.job_registry.active), self.server
        )
        # Register Health service
        health_pb2_grpc.add_HealthServicer_to_server(self.health_servicer, self.server)
        # Set "SERVING" for SolverService
        self.health_servicer.set(
            "SolverService", health_pb2.HealthCheckResponse.SERVING
        )
        service_names = (
            solver_pb2.DESCRIPTOR.services_by_name["SolverService"].full_name,
            health_pb2.DESCRIPTOR.services_by_name["Health"].full_name,
            reflection.SERVICE_NAME,
        )
        reflection.enable_server_reflection(service_names, self.server)
        logger.info(f"Registered gRPC server on {self.SERVER_ADDRESS}")

    def start(self) -> None:
        """
        Runs gRPC server and waits for its termination.

        Register services and starts gRPC server. Logs information about gRPC server start.
        """
        self.register()
        self.job_registry.start()
        logger.info("Job registry started")
        self.server.start()
        logger.info(f"Solver gRPC server started on: {self.SERVER_ADDRESS}")
        self.server.wait_for_termination()

    def stop(self) -> None:
        """
        Stops gRPC server.

        Stops gRPC server without grace period. Logs information about gRPC server stop.
        """
        self.job_registry.stop()
        logger.info("Job registry stopped")
        self.server.stop(grace=False)
        logger.info("gRPC server is stopped")

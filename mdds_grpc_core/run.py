# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
"""
Entry point for running gRPC web server with chosen solver
"""

from grpcserver import GrpcServer
from logging_config import setup_logging


def main():
    """Entry point: start and gracefully stop gRPC server."""
    server = GrpcServer()
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    setup_logging()
    main()

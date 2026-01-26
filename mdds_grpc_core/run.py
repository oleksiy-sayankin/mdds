# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
"""
Entry point for running gRPC web server with chosen solver
"""

import asyncio
from grpcserver import GrpcServer
from logging_config import setup_logging


async def main():
    """Entry point: start and gracefully stop gRPC server."""
    server = GrpcServer()
    try:
        await server.start()
    except KeyboardInterrupt:
        pass
    finally:
        await server.stop()


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())

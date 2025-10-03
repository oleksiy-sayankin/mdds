# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
"""
Entry point for running gRPC server with chosen solver
"""
import os
import asyncio

import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from server import Server


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(Server().run())
    try:
        yield
    finally:
        await Server().stop()


app = FastAPI(
    lifespan=lifespan,
    title="gRPC service on Python",
    description="Solves SLAE using gRPC on Python",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    grpc_host = os.getenv("MDDS_EXECUTOR_GRPC_HOST", "localhost")
    grpc_port = os.getenv("MDDS_EXECUTOR_GRPC_PORT", 50051)

    uvicorn.run(
        "main:app",
        host=grpc_host,
        port=grpc_port,
        reload=True,
    )

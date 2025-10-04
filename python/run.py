# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
"""
Entry point for running gRPC web server with chosen solver
"""
import os
import asyncio

import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from grpcserver import GrpcServer
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

grpc_webclient_dir = os.path.join(os.path.dirname(__file__), "grpc_webclient")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Here we start and stop gRPC Server"""
    asyncio.create_task(GrpcServer().start())
    try:
        yield
    finally:
        await GrpcServer().stop()


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

app.mount(
    "/static", StaticFiles(directory=grpc_webclient_dir, html=True), name="static"
)


@app.get("/")
def index():
    """
    Root endpoint.

    :return: gRPC web server page index.html
    """
    return FileResponse(os.path.join(grpc_webclient_dir, "index.html"))


@app.get("/health")
def health():
    """
    Health check for gRPC web server

    :return: always returns ok as health status
    """
    return {"status": "ok"}


if __name__ == "__main__":
    grpc_webserver_host = os.getenv("MDDS_EXECUTOR_GRPC_WEBSERVER_HOST", "localhost")
    grpc_webserver_port = os.getenv("MDDS_EXECUTOR_GRPC_WEBSERVER_PORT", 40062)

    uvicorn.run(
        "run:app",
        host=grpc_webserver_host,
        port=grpc_webserver_port,
        reload=True,
    )

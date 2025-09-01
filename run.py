# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
"""
Entry point for running server with chosen solver
"""
import uvicorn
from mdds_server.config_loader import Config

if __name__ == "__main__":
    config = Config()
    host = config.get("server", "host", default="0.0.0.0")
    port = config.get("server", "port", default=8000)
    reload = config.get("server", "reload", default="true")

    uvicorn.run(
        "mdds_server.server:app",
        host=host,
        port=port,
        reload=reload,
    )

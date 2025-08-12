# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.
"""
Entry point for running server with chosen solver
"""
import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "mdds_server.server:app",  # full module path
        host="0.0.0.0",
        port=8000,
        reload=True,
    )

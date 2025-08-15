# Use minimal Python 3.12 image
FROM python:3.12-slim

ENV MDDS_HOME=/opt/mdds
ENV MDDS_SERVER_PORT=8000

# Set non-interactive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    libblas-dev  \
    liblapack-dev \
    gcc \
    build-essential         # Used for petsc4py math library

# Upgarde pip to latest
RUN pip install --upgrade pip

# Create app directory
WORKDIR "$MDDS_HOME"

# Clone  GitHub repository
RUN git clone https://github.com/oleksiy-sayankin/mdds.git "$MDDS_HOME"

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose server port
EXPOSE "$MDDS_SERVER_PORT"

# Run the server on container start
CMD ["python", "-m", "run"]

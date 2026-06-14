# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

FROM docker:29.5.3 AS docker-cli
FROM docker/compose-bin:v5.1.4 AS docker-compose
FROM python:3.12-slim AS python-base
FROM eclipse-temurin:21-jdk AS dev-container


COPY --from=python-base /usr/local /usr/local

RUN ldconfig

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    build-essential \
    ca-certificates \
    curl \
    gcc \
    git \
    gnupg \
    jq \
    libblas-dev \
    liblapack-dev \
    libxml2-utils \
    make \
    maven \
    shellcheck \
    shfmt \
    unzip \
    wget \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js 22.
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get update \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Copy Docker CLI binary.
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker

# Copy Docker CLI plugins.
RUN mkdir -p /usr/local/lib/docker/cli-plugins

COPY --from=docker-cli \
    /usr/local/libexec/docker/cli-plugins/docker-buildx \
    /usr/local/lib/docker/cli-plugins/docker-buildx

COPY --from=docker-compose \
    /docker-compose \
    /usr/local/lib/docker/cli-plugins/docker-compose

RUN chmod +x \
    /usr/local/bin/docker \
    /usr/local/lib/docker/cli-plugins/docker-buildx \
    /usr/local/lib/docker/cli-plugins/docker-compose


# Upgrade pip.
RUN python -m pip install --upgrade "pip>=25.3"

# Install production Python dependencies.
COPY deployment/python-base/requirements.txt /tmp/python-base-requirements.txt
COPY deployment/python-base/build-constraints.txt /tmp/build-constraints.txt

RUN python -m pip install --no-cache-dir \
    --build-constraint /tmp/build-constraints.txt \
    -r /tmp/python-base-requirements.txt \
    && python -m pip check

# Install development Python dependencies.
COPY requirements.txt /tmp/dev-requirements.txt

RUN python -m pip install --no-cache-dir \
    --build-constraint /tmp/build-constraints.txt \
    -r /tmp/dev-requirements.txt \
    && python -m pip check

# Install globally available JavaScript CLI tools.
RUN npm install -g newman

# Install google-java-format in the path expected by the current Makefile.
RUN mkdir -p /opt/google-java-format/ \
    && wget https://github.com/google/google-java-format/releases/download/v1.23.0/google-java-format-1.23.0-all-deps.jar \
       -O /opt/google-java-format/google-java-format.jar

# Install Chrome for Web UI tests
RUN install -d -m 0755 /etc/apt/keyrings \
    && wget -qO- https://dl.google.com/linux/linux_signing_key.pub \
      | gpg --dearmor -o /etc/apt/keyrings/google-linux-signing-keyring.gpg \
    && echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/google-linux-signing-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
      > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p \
      /home/ubuntu/.docker \
      /home/ubuntu/.m2 \
      /home/ubuntu/.npm \
      /home/ubuntu/.cache/pip \
    && chown -R ubuntu:ubuntu \
      /home/ubuntu/.docker \
      /home/ubuntu/.m2 \
      /home/ubuntu/.npm \
      /home/ubuntu/.cache

USER ubuntu
WORKDIR /workspaces/mdds

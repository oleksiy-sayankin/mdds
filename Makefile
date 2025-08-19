# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

VENV_DIR := mdds_env
NODE_MODULES := node_modules
PROJECT_ROOT := .
PROJECT_NAME := mdds
USER_NAME := oleksiysayankin
MDDS_SERVER_PORT := 8000
E2E_HOME := tests/e2e

#
# Run server and tests with existing python env
#
run_all:
	make reformat_python
	make check_python_code_style
	make reformat_js
	make check_js_code_style
	make test_and_run

#
# Run tests and start server
#
test_and_run:
	make test_python
	make test_js
	make run_server

#
# Build Docker image
#
build_image:
	echo "[INFO] Building Docker image"
	docker buildx build --tag $(USER_NAME)/$(PROJECT_NAME):latest deployment


#
# Push Docker image
#
push_image:
	echo "[INFO] Pushing Docker image"
	docker push $(USER_NAME)/$(PROJECT_NAME):latest

#
# Reformat JavaScript files
#
reformat_js:
	echo "[INFO] Reformating JavaScript sources"
	prettier --write mdds_client/

#
# Check python code style
#
check_python_code_style:
	echo "[INFO] Checking python code style"
	pycodestyle $(PROJECT_ROOT) --exclude=*$(VENV_DIR)*,*$(NODE_MODULES) --ignore=E501
	ruff check $(PROJECT_ROOT) --fix --force-exclude $(VENV_DIR) --respect-gitignore
	pylint $(PROJECT_ROOT) --ignore $(VENV_DIR) --errors-only

#
# Check JavaScript code style
#
check_js_code_style:
	echo "[INFO] Checking JavaScript code style"
	eslint mdds_client/ --debug --fix

#
# Reformat Python code
#
reformat_python:
	echo "[INFO] Reformating python sources"
	black .  --exclude '/($(VENV_DIR)|$(NODE_MODULES))/' --verbose

#
# Run Python tests
#
test_python:
	echo "[INFO] Running Python unit tests"
	pytest -v

#
# Run JavaScript tests
#
test_js:
	echo "[INFO] Running JavaScript unit tests"
	npm test

#
# Run server
#
run_server:
	echo "[INFO] Starting web-server"
	python -m run

#
# Start Docker container and run end to end tests
#
setup_and_run_e2e:
	echo "[INFO] Starting up environment"
	echo "MDDS_SERVER_PORT=$(MDDS_SERVER_PORT)" > $(E2E_HOME)/.env
	docker-compose -f $(E2E_HOME)/docker-compose.yml up -d
	echo "[INFO] Running end to end tests"
	npm run test:e2e
	echo "[INFO] Shutting down environment"
	docker-compose -f $(E2E_HOME)/docker-compose.yml down

#
# Setup python environment
#
setup_python_env:
	make clear_python_env
	make create_python_env
	make install_python_libs

#
# Clear python environment
#
clear_python_env:
	echo "[INFO] Clearing python environment"
	rm -Rf $(VENV_DIR)

#
# Create python environment
#
create_python_env:
	echo "[INFO] Creating python environment $(VENV_DIR)"
	python3 -m venv $(VENV_DIR)

#
# Install python libraries
#
install_python_libs:
	echo "[INFO] Installing python libraries"
	. $(VENV_DIR)/bin/activate; pip install -r requirements.txt

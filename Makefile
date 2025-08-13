# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

VENV_DIR := mdds_env
PROJECT_ROOT := .

#
# Run server and tests with existing python env
#
run_all:
	make reformat
	make check_code_style
	make test_and_run

#
# Run tests and start server
#
test_and_run:
	make test
	make run_server

#
# Check code style
#
check_code_style:
	echo "[INFO] Checking code style"
	pycodestyle $(PROJECT_ROOT) --exclude=*$(VENV_DIR)* --ignore=E501
	ruff check $(PROJECT_ROOT) --fix --force-exclude $(VENV_DIR) --respect-gitignore
	pylint $(PROJECT_ROOT) --ignore $(VENV_DIR) --errors-only

#
# Reformat code
#
reformat:
	echo "[INFO] Reformating python sources"
	black .  --exclude $(VENV_DIR)/ --verbose
#
# Run tests
#
test:
	echo "[INFO] Running unit tests"
	pytest -v

#
# Run server
#
run_server:
	echo "[INFO] Starting web-server"
	python -m run

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

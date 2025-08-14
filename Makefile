# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

VENV_DIR := mdds_env
NODE_MODULES := node_modules
PROJECT_ROOT := .

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
	make test
	make run_server

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

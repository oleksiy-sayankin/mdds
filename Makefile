# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

PYTHON_ENV_HOME := ~/.venvs
NODE_MODULES := node_modules
PROJECT_ROOT := .
PROJECT_NAME := mdds
VENV_DIR := $(PYTHON_ENV_HOME)/$(PROJECT_NAME)
USER_NAME := oleksiysayankin
MDDS_SERVER_PORT := 8000
MDDS_SERVER_HOST := localhost
E2E_HOME := tests/e2e
CHECK_LICENSE_STRING = "Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved."

#
# Run server and tests with existing python env
#
run_all: reformat_and_check_all test_and_run

#
# Reformat and check all code
#
reformat_and_check_all: check_license reformat_python check_python_code_style reformat_js check_js_code_style reformat_bash check_bash_code_style

#
# Run tests and start server
#
test_and_run: test_python test_js run_server

#
# Build Docker image
#
build_image:
	@echo "[INFO] Building Docker image..."
	docker buildx build --no-cache --progress=plain --tag $(USER_NAME)/$(PROJECT_NAME):latest deployment
	@echo "✅ [INFO] Building Docker image completed."


#
# Push Docker image
#
push_image:
	@echo "[INFO] Pushing Docker image..."
	docker push $(USER_NAME)/$(PROJECT_NAME):latest
	@echo "✅ [INFO] Pushing Docker image completed."

#
# Reformat JavaScript files
#
reformat_js:
	@echo "[INFO] Reformating JavaScript sources..."
	prettier --write mdds_client/
	@echo "✅ [INFO] Reformating JavaScript sources completed."

#
# Check python code style
#
check_python_code_style:
	@echo "[INFO] Checking python code style..."
	pycodestyle $(PROJECT_ROOT) --exclude=*$(VENV_DIR)*,*$(NODE_MODULES) --ignore=E501
	ruff check $(PROJECT_ROOT) --fix --force-exclude $(VENV_DIR) --respect-gitignore
	pylint $(PROJECT_ROOT) --ignore $(VENV_DIR) --errors-only
	@echo "✅ [INFO] Checking python code style completed."

#
# Check JavaScript code style
#
check_js_code_style:
	@echo "[INFO] Checking JavaScript code style..."
	eslint mdds_client/ --debug --fix
	@echo "✅ [INFO] Checking JavaScript code style completed."

#
# Reformat Python code
#
reformat_python:
	@echo "[INFO] Reformating python sources..."
	black .  --exclude '/($(VENV_DIR)|$(NODE_MODULES))/' --verbose
	@echo "✅ [INFO] Reformating python sources completed."

#
# Check bash code style
#
check_bash_code_style:
	@echo "[INFO] Checking Bash code style..."
	find . ! -path "*$(NODE_MODULES)*" -type f \( -name "*.sh" -o -name "*.bats" \) | xargs shellcheck
	@echo "✅ [INFO] Checking Bash code style completed."

#
# Reformat bash shell scripts
#
reformat_bash:
	@echo "[INFO] Reformating bash sources..."
	find . ! -path "*$(NODE_MODULES)*" -type f \( -name "*.sh" -o -name "*.bats" \) |xargs shfmt  -i 2 -w
	@echo "✅ [INFO] Reformating bash shell scripts completed."

#
# Run Python tests
#
test_python:
	@echo "[INFO] Running Python unit tests..."
	pytest -v
	@echo "✅ [INFO] Python tests completed."

#
# Run JavaScript tests
#
test_js:
	@echo "[INFO] Running JavaScript unit tests..."
	if ! npm test; then \
		echo "❌ [ERROR] JS tests failed"; \
		exit 1; \
	fi
	@echo "✅ [INFO] JavaScript tests completed."

#
# Run server
#
run_server:
	@echo "[INFO] Starting web-server..."
	python -m run
	@echo "✅ [INFO] Starting web-server completed. Web-server is up!"

#
# Wait until server is ready by checking its health state
#
wait_for_server:
	@retry_count=10; \
	current_retry=1; \
	echo "[INFO] Waiting for server on port $(MDDS_SERVER_PORT)..."; \
	while [ $$current_retry -le $$retry_count ]; do \
		if curl -s http://$(MDDS_SERVER_HOST):$(MDDS_SERVER_PORT)/health > /dev/null; then \
			echo "✅ [INFO] Server is up!"; \
			exit 0; \
		else \
			echo "[INFO] Attempt $$current_retry/$$retry_count: Server not ready yet, retrying..."; \
			current_retry=$$((current_retry+1)); \
			sleep 2; \
		fi; \
	done; \
	echo "❌ [ERROR] Server did not become ready after $$retry_count attempts"; \
	exit 1

#
# Start Docker container and run end to end tests
#
setup_and_run_e2e:
	@echo "[INFO] Starting up environment..."
	echo "MDDS_SERVER_PORT=$(MDDS_SERVER_PORT)" > $(E2E_HOME)/.env
	docker compose -f $(E2E_HOME)/docker-compose.yml up -d
	$(MAKE) wait_for_server
	@echo "[INFO] Running end to end tests..."
	npm run test:e2e
	@echo "[INFO] Shutting down environment..."
	docker compose -f $(E2E_HOME)/docker-compose.yml down
	@echo "✅ [INFO] End to end tests completed."

#
# Setup python environment
#
setup_python_env: clear_python_env create_python_env install_python_libs

#
# Clear python environment
#
clear_python_env:
	@echo "[INFO] Clearing python environment $(VENV_DIR)..."
	rm -Rf $(VENV_DIR)
	@echo "✅ [INFO] Clearing python environment $(VENV_DIR) completed.")

#
# Create python environment
#
create_python_env:
	@echo "[INFO] Creating python environment $(VENV_DIR)..."
	mkdir -p $(VENV_DIR)
	python3 -m venv $(VENV_DIR)
	@echo "✅ [INFO] Creating python environment $(VENV_DIR) completed."

#
# Install python libraries
#
install_python_libs:
	@echo "[INFO] Installing python libraries..."
	. $(VENV_DIR)/bin/activate; pip install -r requirements.txt
	@echo "✅ [INFO] Installing python libraries competed."


#
# Check that every file in project has license header
#
check_license:
	@echo "🔍 [INFO] Checking license headers in source files..."
	@FILES=$$(find . \
		-type f \
		-not -path "./node_modules/*" \
		-not -path "./.idea/*" \
		-not -path "*/__pycache__/*" \
		-not -path "*/.pytest_cache/*" \
		-not -path "*/.ruff_cache/*" \
		-not -path "*/.git/*" \
		-not -path "./logs/*" \
		-not -name "*.csv" \
		-not -name "*.ico" \
		-not -name "*.gitignore" \
		-not -name "*.json" \
		-not -name "*.env" \
		-not -name "__init__.py"); \
	STATUS=0; \
	for f in $$FILES; do \
		if ! grep -q $(CHECK_LICENSE_STRING) $$f; then \
			echo "❌ [ERROR] Missing license in: $$f"; \
			STATUS=1; \
		fi; \
	done; \
	if [ $$STATUS -eq 0 ]; then \
		echo "✅ [INFO] All files contain license header."; \
	else \
		echo "⚠️ [ERROR] Some files are missing license header."; \
		exit 1; \
	fi

# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

PYTHON_ENV_HOME := ~/.venvs
PYTHON_BIN ?= python3.12
PYTHON_BUILD_CONSTRAINTS := deployment/python-base/build-constraints.txt
NODE_MODULES := node_modules
BUILD := build
TARGET := target
MDDS_WEB_SERVER := mdds-web-server
PROJECT_ROOT := .
PROJECT_NAME := mdds
PROJECT_VERSION := 0.1.0

MDDS_WORKER_RUNTIME := mdds-python-worker-runtime
WORKER_RUNTIME_ROOT := $(PROJECT_ROOT)/$(MDDS_WORKER_RUNTIME)
WORKER_RUNTIME_MAIN := $(WORKER_RUNTIME_ROOT)/src/main/python
WORKER_RUNTIME_TEST := $(WORKER_RUNTIME_ROOT)/src/test/python
WORKER_RUNTIME_PACKAGE := $(MDDS_WORKER_RUNTIME)
WORKER_RUNTIME_DIST := $(WORKER_RUNTIME_PACKAGE)/target/dist

MDDS_WORKER_SLAE := mdds-python-worker-solving-slae
WORKER_SLAE_ROOT := $(PROJECT_ROOT)/mdds-examples/workers/$(MDDS_WORKER_SLAE)
WORKER_SLAE_MAIN := $(WORKER_SLAE_ROOT)/src/main/python
WORKER_SLAE_TEST := $(WORKER_SLAE_ROOT)/src/test/python
WORKER_SLAE_PACKAGE := $(WORKER_SLAE_ROOT)
WORKER_SLAE_DIST := $(WORKER_SLAE_ROOT)/target/dist

MDDS_E2E_TESTS := mdds-e2e-tests
E2E_TESTS_ROOT := $(PROJECT_ROOT)/$(MDDS_E2E_TESTS)
E2E_TESTS_MAIN := $(E2E_TESTS_ROOT)/src/main/python
E2E_TESTS_TEST := $(E2E_TESTS_ROOT)/src/test/python
E2E_TESTS_NEWMAN_HOME := $(MDDS_E2E_TESTS)/newman
E2E_TESTS_PROJECT_NAME := mdds-e2e
E2E_TESTS_COMPOSE := docker compose --project-name $(E2E_TESTS_PROJECT_NAME) --progress=plain -f $(E2E_TESTS_NEWMAN_HOME)/docker-compose.yml

PYTHON_BASE_REQUIREMENTS := deployment/python-base/requirements.txt
PYTHON_BASE_BUILD_CONSTRAINTS := deployment/python-base/build-constraints.txt
PYTHON_BASE_REQUIREMENTS_LOCK := deployment/python-base/requirements.lock.txt


COMMON_WEB_CLIENT_ROOT := ./mdds-examples/web-clients/mdds-common-web-client
COMMON_WEB_CLIENT_PACKAGE_JSON := $(COMMON_WEB_CLIENT_ROOT)/package.json
COMMON_WEB_CLIENT_PACKAGE_LOCK := $(COMMON_WEB_CLIENT_ROOT)/package-lock.json
COMMON_WEB_CLIENT_NODE_MODULES_LOCK := \
	$(COMMON_WEB_CLIENT_ROOT)/$(NODE_MODULES)/.package-lock.json

WEB_APP_DIR := $(PROJECT_ROOT)/$(MDDS_WEB_SERVER)/src/main/resources/static
JAVA_ROOT := $(PROJECT_ROOT)
VENV_DIR := $(PYTHON_ENV_HOME)/$(PROJECT_NAME)
USER_NAME := mddsproject
DEPLOYMENT_DIR := deployment
DEPLOYMENT_TEST_ROOT := $(PROJECT_ROOT)/$(DEPLOYMENT_DIR)/test
MDDS_E2E_SERVER_HOST ?= web-server
MDDS_E2E_SERVER_PORT ?= 8000
DEMO_HOME := mdds-demo
GOOGLE_JAVA_FORMAT_HOME := /opt/google-java-format
DOCKER_GID ?= $(shell stat -c '%g' /var/run/docker.sock)
OBSERVABILITY_DEPLOYMENT_DIR := $(DEPLOYMENT_DIR)/observability
ALLOY_DEPLOYMENT_DIR := $(OBSERVABILITY_DEPLOYMENT_DIR)/alloy
LOKI_DEPLOYMENT_DIR := $(OBSERVABILITY_DEPLOYMENT_DIR)/loki
GRAFANA_DEPLOYMENT_DIR := $(OBSERVABILITY_DEPLOYMENT_DIR)/grafana
SONAR_HOST_URL ?= http://localhost:9021
SONAR_PROJECT_KEY ?= mdds
SONAR_TOKEN_FILE ?= .sonar_token
SONAR_TOKEN ?= $(shell test -s $(SONAR_TOKEN_FILE) && tr -d '\n' < $(SONAR_TOKEN_FILE) || true)
SONAR_ADMIN_LOGIN ?= admin
SONAR_DEFAULT_ADMIN_PASSWORD ?= admin
SONAR_ADMIN_PASSWORD ?= MddsLocalSonarAdmin2026_A9xQ7mZ2
SONAR_TOKEN_NAME ?= mdds-local-sonar-token
CHECK_LICENSE_STRING = "Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved."

# Colors for output text.
RED=\e[31m
GREEN=\e[32m
YELLOW=\e[33m
BLUE=\e[34m
NC=\e[0m
BOLD=\e[1m

define log_info
	@echo "[${BLUE}${BOLD}INFO${NC}] $(1)"
endef

define log_info_sh
	echo "[${BLUE}${BOLD}INFO${NC}] $(1)"
endef

define log_done
	@echo "[${BLUE}${BOLD}INFO${NC}] ✅ $(1)"
endef

define log_done_sh
	echo "[${BLUE}${BOLD}INFO${NC}] ✅ $(1)"
endef

define log_error
	@echo "[${RED}${BOLD}ERROR${NC}] ❌ $(1)"
endef

define log_error_sh
	echo "[${RED}${BOLD}ERROR${NC}] ❌ $(1)"
endef


#
# Comprehensive local project verification.
#
# Rebuilds all project artifacts from clean source directories, builds all
# release Docker images, runs formatting, static checks, coverage, E2E tests
# and SonarQube analysis.
#
.PHONY: \
	clean_build_artifacts \
	reformat_all \
	check_all \
	validate_compose_files \
	build_all_packages \
	build_all_images \
	verify_all_from_scratch \
	run_all


#
# Backward-compatible alias for the main verification target.
#
run_all: verify_all_from_scratch

#
# Remove generated project artifacts.
#
# Dependency caches and Docker layer caches are intentionally preserved.
#
clean_build_artifacts:
	$(call log_info,"Cleaning all generated project artifacts...")
	mvn clean
	rm -rf \
		target \
		$(WORKER_RUNTIME_ROOT)/build \
		$(WORKER_RUNTIME_ROOT)/target \
		$(WORKER_RUNTIME_ROOT)/src/main/python/*.egg-info \
		$(WORKER_SLAE_ROOT)/build \
		$(WORKER_SLAE_ROOT)/target \
		$(WORKER_SLAE_ROOT)/src/main/python/*.egg-info \
		$(E2E_TESTS_ROOT)/target \
		$(COMMON_WEB_CLIENT_ROOT)/target \
		$(COMMON_WEB_CLIENT_ROOT)/$(NODE_MODULES)
	$(call log_done,"Cleaning all generated project artifacts completed.")


#
# Reformat all supported source files.
#
reformat_all:
	@$(MAKE) reformat_java
	@$(MAKE) reformat_xml
	@$(MAKE) reformat_python
	@$(MAKE) reformat_common_web_client
	@$(MAKE) reformat_bash


#
# Validate Docker Compose files.
#
validate_compose_files:
	$(call log_info,"Validating Docker Compose files...")
	DOCKER_GID=$(DOCKER_GID) docker compose \
		--progress=plain \
		-f $(DEMO_HOME)/compose.demo.yml \
		config --quiet
	$(E2E_TESTS_COMPOSE) config --quiet
	$(call log_done,"Docker Compose files are valid.")


#
# Run all static checks.
#
check_all:
	@$(MAKE) check_license
	@$(MAKE) check_python_code_style
	@$(MAKE) check_common_web_client_code_style
	@$(MAKE) check_bash_code_style
	@$(MAKE) validate_compose_files
	git diff --check


#
# Build all project packages without building Docker images.
#
build_all_packages:
	@$(MAKE) build_jars_ci
	@$(MAKE) package_python_workers
	@$(MAKE) package_common_web_client


#
# Build all project Docker images.
#
# The demo stack uses these same Web App and SLAE Worker images; there is no
# separate category of custom "demo images".
#
build_all_images:
	@$(MAKE) build_java_base_docker_image
	@$(MAKE) build_python_base_docker_image
	@$(MAKE) build_web_server_docker_image
	@$(MAKE) build_web_app_docker_image
	@$(MAKE) build_python_worker_runtime_docker_image
	@$(MAKE) build_python_worker_solving_slae_docker_image
	@$(MAKE) build_observability_images


#
# Rebuild and verify the complete project.
#
verify_all_from_scratch:
	$(call log_info,"Starting complete MDDS rebuild and verification...")
	@$(MAKE) clean_build_artifacts
	@$(MAKE) reformat_all
	@$(MAKE) check_all
	@$(MAKE) build_all_packages
	@$(MAKE) build_all_images
	@$(MAKE) test_coverage
	@$(MAKE) test_e2e
	@SONAR_QUALITYGATE_WAIT=false $(MAKE) sonar_scan
	$(call log_done,"Complete MDDS rebuild and verification passed.")


#
# Run tests and start server
#
test_and_run: test_all \
	start_mdds_env

#
# Run all tests
#
test_all: test_python \
	test_java \
	test_e2e

#
# Run CVE scan for Java jars
#
scan_jars_for_cve:
	$(call log_info,"Scanning jars for CVE...")

	@mvn -Dmaven.jvm.args="--enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector" \
		org.owasp:dependency-check-maven:check \
		-DoutputDirectory=target/dependency-check-report \
		-q

	@TOTAL_COUNT=$$(jq '[.dependencies[]?.vulnerabilities[]? ] | length' \
    		target/dependency-check-report.json 2>/dev/null || echo 0);	\
    $(call log_info_sh,"Total CVE count : $$TOTAL_COUNT")

	$(call log_info,"Found CVEs:")
	@jq -r '.dependencies[]?.vulnerabilities[]? | "ID: \(.name)\nSeverity: \(.severity)\nDescription: \(.description)\n---"' \
		target/dependency-check-report.json || true

	@CRIT_COUNT=$$(jq '[.dependencies[]?.vulnerabilities[]? | select((.severity | ascii_downcase) == "critical")] | length' \
		target/dependency-check-report.json 2>/dev/null || echo 0); \
	if [ "$$CRIT_COUNT" -gt 0 ]; then \
		$(call log_error_sh, "Found $$CRIT_COUNT CRITICAL vulnerabilities! Failing build."); \
		exit 1; \
	else \
		$(call log_info_sh,"No CRITICAL vulnerabilities found. Build passes."); \
	fi
	$(call log_done,"Scanning jars for CVE completed.")


#
# Build Docker image for all Java based containers
#
build_java_base_docker_image:
	$(call log_info,"Building Java base Docker image...")
	docker buildx build -f deployment/java-base/Dockerfile --progress=plain --tag $(USER_NAME)/java-base:$(PROJECT_VERSION) .
	$(call log_done,"Building Java base Docker image completed.")

#
# Push Java base Docker image
#
push_java_base_docker_image:
	$(call log_info,"Pushing Java base Docker image...")
	docker push $(USER_NAME)/java-base:$(PROJECT_VERSION)
	$(call log_done,"Pushing Java base Docker image completed.")

#
# Generate Python base requirements lock file
#
.PHONY: lock_python_base_requirements
lock_python_base_requirements:
	@echo "[INFO] Generating Python base requirements lock file..."
	CUSTOM_COMPILE_COMMAND="make lock_python_base_requirements" \
	python -m piptools compile \
		--generate-hashes \
		--allow-unsafe \
		--resolver=backtracking \
		--constraint $(PYTHON_BASE_BUILD_CONSTRAINTS) \
		--output-file $(PYTHON_BASE_REQUIREMENTS_LOCK) \
		$(PYTHON_BASE_REQUIREMENTS)
	@echo "[INFO] ✅ Python base requirements lock file generated."

#
# Build common Docker that is use as root image for Python images
#
build_python_base_docker_image:
	$(call log_info,"Building Python base Docker image...")
	docker buildx build -f deployment/python-base/Dockerfile --progress=plain --tag $(USER_NAME)/python-base:$(PROJECT_VERSION) .
	$(call log_done,"Building Python base Docker image completed.")

#
# Push Python base Docker image
#
push_python_base_docker_image:
	$(call log_info,"Pushing  Python base Docker image...")
	docker push $(USER_NAME)/python-base:$(PROJECT_VERSION)
	$(call log_done,"Pushing Python base Docker image completed.")


#
# Build Docker image for Python Worker Runtime. Requires an existing Python base image and a built Worker Runtime package.
#
build_python_worker_runtime_docker_image:
	$(call log_info,"Building Python Worker Runtime Docker image...")
	docker buildx build -f deployment/python-worker-runtime/Dockerfile --progress=plain --tag $(USER_NAME)/python-worker-runtime:$(PROJECT_VERSION) .
	$(call log_done,"Building Python Worker Runtime Docker image completed.")


#
# Push Docker image for Python Worker Runtime
#
push_python_worker_runtime_docker_image:
	$(call log_info,"Pushing Docker image for Python Worker Runtime...")
	docker push $(USER_NAME)/python-worker-runtime:$(PROJECT_VERSION)
	$(call log_done,"Pushing Docker image for Python Worker Runtime.")


#
# Build Docker image for the SLAE Python Worker.
# Requires the Python Worker Runtime Docker image and the SLAE worker wheel package.
#
build_python_worker_solving_slae_docker_image:
	$(call log_info,"Building Python Worker solving SLAE Docker image...")
	docker buildx build -f deployment/python-worker-solving-slae/Dockerfile --progress=plain --tag $(USER_NAME)/python-worker-solving-slae:$(PROJECT_VERSION) .
	$(call log_done,"Building Python Worker solving SLAE Docker image completed.")


#
# Push Docker image for Python Worker solving SLAE
#
push_python_worker_solving_slae_docker_image:
	$(call log_info,"Pushing Docker image for Python Worker solving SLAE...")
	docker push $(USER_NAME)/python-worker-solving-slae:$(PROJECT_VERSION)
	$(call log_done,"Pushing Docker image for Python Worker solving SLAE.")


#
# Build Developer container Docker image
#
build_dev_container_docker_image:
	$(call log_info,"Building Developer container Docker image...")
	docker buildx build -f .devcontainer/Dockerfile --progress=plain --tag $(USER_NAME)/dev-container:$(PROJECT_VERSION) .
	$(call log_done,"Building Developer container Docker image completed.")

#
# Push Developer container Docker image
#
push_dev_container_docker_image:
	$(call log_info,"Pushing Developer container Docker image...")
	docker push $(USER_NAME)/dev-container:$(PROJECT_VERSION)
	$(call log_done,"Pushing Developer container Docker image completed.")


#
# Start development shell container
#
start_dev_shell:
	$(call log_info,"Starting MDDS development shell container...")
	@DOCKER_GID=$(DOCKER_GID) docker compose -f mdds-dev/compose.dev.yml up -d
	$(call log_done,"MDDS development shell container is up.")

#
# Stop development shell container
#
stop_dev_shell:
	$(call log_info,"Stopping MDDS development shell container...")
	@DOCKER_GID=$(DOCKER_GID) docker compose -f mdds-dev/compose.dev.yml down
	$(call log_done,"MDDS development shell container stopped.")

#
# Open interactive shell inside development container
#
dev_shell: start_dev_shell
	@docker exec -it mdds-dev-shell bash



#
# Build Docker image for web-server
#
build_web_server_docker_image:
	$(call log_info,"Building Docker image for web-server...")
	docker buildx build -f deployment/web-server/Dockerfile --progress=plain --tag $(USER_NAME)/web-server:$(PROJECT_VERSION) .
	$(call log_done,"Building Docker image for web-server completed.")

#
# Push web-server Docker image
#
push_web_server_docker_image:
	$(call log_info,"Pushing web-server Docker image...")
	docker push $(USER_NAME)/web-server:$(PROJECT_VERSION)
	$(call log_done,"Pushing web-server Docker image completed.")


#
# Build Docker image for Web Application
#
build_web_app_docker_image:
	$(call log_info,"Building Docker image for Web Application...")
	docker buildx build -f deployment/web-app/Dockerfile --progress=plain --tag $(USER_NAME)/web-app:$(PROJECT_VERSION) .
	$(call log_done,"Building Docker image for Web Application completed.")

#
# Push Web Application Docker image
#
push_web_app_docker_image:
	$(call log_info,"Pushing web-app Docker image ...")
	docker push $(USER_NAME)/web-app:$(PROJECT_VERSION)
	$(call log_done,"Pushing web-app Docker image completed.")

#
# Install Common Web Client dependencies
#
$(COMMON_WEB_CLIENT_NODE_MODULES_LOCK): \
		$(COMMON_WEB_CLIENT_PACKAGE_JSON) \
		$(COMMON_WEB_CLIENT_PACKAGE_LOCK)
	$(call log_info,"Installing Common Web Client dependencies...")
	npm ci \
		--prefix $(COMMON_WEB_CLIENT_ROOT) \
		--include=dev \
		--ignore-scripts \
		--no-audit \
		--no-fund
	$(call log_done,"Common Web Client dependencies installed.")


.PHONY: install_common_web_client_dependencies
install_common_web_client_dependencies: \
	$(COMMON_WEB_CLIENT_NODE_MODULES_LOCK)



#
# Reformat Common Web Client
#
reformat_common_web_client: install_common_web_client_dependencies
	cd $(COMMON_WEB_CLIENT_ROOT) && npm run format


#
# Check code style
#
check_common_web_client_code_style: install_common_web_client_dependencies
	cd $(COMMON_WEB_CLIENT_ROOT) && npm run code:check

#
# Run Common Web Client tests
#
test_common_web_client: install_common_web_client_dependencies
	cd $(COMMON_WEB_CLIENT_ROOT) && npm test


#
# Run Common Web Client tests with TypeScript coverage.
#
test_common_web_client_coverage: install_common_web_client_dependencies
	$(call log_info,"Running Common Web Client tests with TypeScript coverage...")
	cd $(COMMON_WEB_CLIENT_ROOT) && npm run test:coverage
	@test -s $(COMMON_WEB_CLIENT_ROOT)/target/coverage/lcov.info
	$(call log_done,"Common Web Client tests with TypeScript coverage completed.")

#
# Build Common Web Client
#
build_common_web_client: install_common_web_client_dependencies
	$(call log_info,"Building Common Web Client...")
	cd $(COMMON_WEB_CLIENT_ROOT) && npm run build
	@test -f $(COMMON_WEB_CLIENT_ROOT)/target/common-web-client-dist/index.html
	$(call log_done,"Building Common Web Client completed.")

#
# Package Common Web Client
#
package_common_web_client: build_common_web_client
	$(call log_info,"Packaging Common Web Client...")
	tar --create \
		--gzip \
		--file $(COMMON_WEB_CLIENT_ROOT)/target/mdds-common-web-client.tar.gz \
		--directory $(COMMON_WEB_CLIENT_ROOT)/target/common-web-client-dist \
		.
	@test -s $(COMMON_WEB_CLIENT_ROOT)/target/mdds-common-web-client.tar.gz
	$(call log_done,"Packaging Common Web Client completed.")


.PHONY: \
	build_alloy_docker_image \
	push_alloy_docker_image \
	build_loki_docker_image \
	push_loki_docker_image \
	build_grafana_docker_image \
	push_grafana_docker_image \
	build_observability_images \
	push_observability_images \
	build_and_push_observability_images \
	build_release_images \
	build_release_images_ci \
	push_release_images \
	build_and_push_release_images \
	reformat_common_web_client \
	check_common_web_client_code_style \
	test_common_web_client \
	test_coverage \
	build_common_web_client \
	package_common_web_client \
	build_web_app_docker_image \
	push_web_app_docker_image \
	test_common_web_client_coverage


#
# Build Alloy Docker image
#
build_alloy_docker_image:
	$(call log_info,"Building Alloy Docker image...")
	docker buildx build \
		-f $(ALLOY_DEPLOYMENT_DIR)/Dockerfile \
		--progress=plain \
		--tag $(USER_NAME)/alloy:$(PROJECT_VERSION) \
		$(ALLOY_DEPLOYMENT_DIR)
	$(call log_done,"Building Alloy Docker image completed.")


#
# Push Alloy Docker image
#
push_alloy_docker_image:
	$(call log_info,"Pushing Alloy Docker image...")
	docker push $(USER_NAME)/alloy:$(PROJECT_VERSION)
	$(call log_done,"Pushing Alloy Docker image completed.")


#
# Build Loki Docker image
#
build_loki_docker_image:
	$(call log_info,"Building Loki Docker image...")
	docker buildx build \
		-f $(LOKI_DEPLOYMENT_DIR)/Dockerfile \
		--progress=plain \
		--tag $(USER_NAME)/loki:$(PROJECT_VERSION) \
		$(LOKI_DEPLOYMENT_DIR)
	$(call log_done,"Building Loki Docker image completed.")


#
# Push Loki Docker image
#
push_loki_docker_image:
	$(call log_info,"Pushing Loki Docker image...")
	docker push $(USER_NAME)/loki:$(PROJECT_VERSION)
	$(call log_done,"Pushing Loki Docker image completed.")


#
# Build Grafana Docker image
#
build_grafana_docker_image:
	$(call log_info,"Building Grafana Docker image...")
	docker buildx build \
		-f $(GRAFANA_DEPLOYMENT_DIR)/Dockerfile \
		--progress=plain \
		--tag $(USER_NAME)/grafana:$(PROJECT_VERSION) \
		$(GRAFANA_DEPLOYMENT_DIR)
	$(call log_done,"Building Grafana Docker image completed.")


#
# Push Grafana Docker image
#
push_grafana_docker_image:
	$(call log_info,"Pushing Grafana Docker image...")
	docker push $(USER_NAME)/grafana:$(PROJECT_VERSION)
	$(call log_done,"Pushing Grafana Docker image completed.")


#
# Build all observability Docker images
#
build_observability_images: \
		build_alloy_docker_image \
		build_loki_docker_image \
		build_grafana_docker_image


#
# Push all observability Docker images
#
push_observability_images: \
		push_alloy_docker_image \
		push_loki_docker_image \
		push_grafana_docker_image

#
# Build all release Docker images
#
build_release_images: \
		build_java_base_docker_image \
		build_python_base_docker_image \
		build_main_images \
		build_observability_images

#
# Build all release Docker images for CI
#
build_release_images_ci: \
		build_java_base_docker_image \
		build_python_base_docker_image \
		build_main_images_ci \
		build_observability_images

#
# Push all release Docker images
#
push_release_images: \
		push_java_base_docker_image \
		push_python_base_docker_image \
		push_main_images \
		push_observability_images

#
# Build and push all release Docker images
#
build_and_push_release_images: build_release_images \
	push_release_images

#
# Build and push all observability Docker images
#
build_and_push_observability_images: \
		build_observability_images \
		push_observability_images

#
# Build main images. Here we do not build base Java and Python docker images since they are rarely changed.
#
build_main_images: build_jars \
	build_web_server_docker_image \
	build_common_web_client \
	package_common_web_client \
	build_web_app_docker_image \
	package_python_workers \
	build_python_worker_runtime_docker_image \
	build_python_worker_solving_slae_docker_image


#
# Build main Docker images without formatting or auto-fixing sources.
# Intended for CI and reproducible builds.
#
build_main_images_ci: build_jars_ci \
	build_web_server_docker_image \
	build_common_web_client \
	package_common_web_client \
	build_web_app_docker_image \
	package_python_workers \
	build_python_worker_runtime_docker_image \
	build_python_worker_solving_slae_docker_image


#
# Push main images.
#
push_main_images: push_web_server_docker_image \
	push_web_app_docker_image \
	push_python_worker_runtime_docker_image \
	push_python_worker_solving_slae_docker_image

#
# Build and push main images.
#
build_and_push_main_images: build_main_images \
	push_main_images

#
# Reformat all Python code
#
reformat_python: reformat_worker_runtime \
	reformat_worker_slae \
	reformat_e2e_tests

#
# Check code style for all Python code
#
check_python_code_style: check_worker_runtime_code_style \
	check_worker_slae_code_style \
	check_e2e_code_style

#
# Test all Python code
#
test_python_coverage: test_worker_runtime_coverage \
	test_worker_slae_coverage

#
# Check worker runtime code style
#
check_worker_runtime_code_style:
	$(call log_info,"Checking worker runtime code style...")
	pycodestyle $(WORKER_RUNTIME_ROOT) --exclude=*$(VENV_DIR)*,*$(BUILD)*,*$(TARGET)*,*$(NODE_MODULES) --ignore=E501,W503
	ruff check $(WORKER_RUNTIME_ROOT) --fix --force-exclude --respect-gitignore
	PYTHONPATH=$(WORKER_RUNTIME_MAIN):$(WORKER_RUNTIME_TEST):$$PYTHONPATH pylint $(WORKER_RUNTIME_ROOT)/ --ignore $(VENV_DIR),$(BUILD),$(TARGET),$(NODE_MODULES) --errors-only
	$(call log_done,"Checking worker runtime code style completed.")


#
# Check worker slae code style
#
check_worker_slae_code_style:
	$(call log_info,"Checking worker slae style...")
	pycodestyle $(WORKER_SLAE_ROOT) --exclude=*$(VENV_DIR)*,*$(BUILD)*,*$(TARGET)*,*$(NODE_MODULES) --ignore=E501,W503
	ruff check $(WORKER_SLAE_ROOT) --fix --force-exclude --respect-gitignore
	PYTHONPATH=$(abspath $(WORKER_RUNTIME_MAIN)):$(WORKER_SLAE_MAIN):$(WORKER_SLAE_TEST):$$PYTHONPATH pylint $(WORKER_SLAE_ROOT)/ --ignore $(VENV_DIR),$(BUILD),$(TARGET),$(NODE_MODULES) --errors-only
	$(call log_done,"Checking worker slae style completed.")


#
# Check e2e sources code style
#
check_e2e_code_style:
	$(call log_info,"Checking e2e sources style...")
	pycodestyle $(E2E_TESTS_ROOT) --exclude=*$(VENV_DIR)*,*$(BUILD)*,*$(TARGET)*,*$(NODE_MODULES) --ignore=E501,W503
	ruff check $(E2E_TESTS_ROOT) --fix --force-exclude --respect-gitignore
	PYTHONPATH=$(abspath $(E2E_TESTS_MAIN)):$(E2E_TESTS_MAIN):$(E2E_TESTS_TEST):$$PYTHONPATH pylint $(E2E_TESTS_ROOT)/ --ignore $(VENV_DIR),$(BUILD),$(TARGET),$(NODE_MODULES) --errors-only
	$(call log_done,"Checking e2e sources completed.")


#
# Reformat worker runtime code
#
reformat_worker_runtime:
	$(call log_info,"Reformating worker runtime sources...")
	black $(WORKER_RUNTIME_ROOT)  --exclude '/($(VENV_DIR)|$(NODE_MODULES)|$(BUILD)|$(TARGET))/' --verbose
	$(call log_done,"Reformating worker runtime sources completed.")


#
# Reformat worker slae code
#
reformat_worker_slae:
	$(call log_info,"Reformating worker slae sources...")
	black $(WORKER_SLAE_ROOT)  --exclude '/($(VENV_DIR)|$(NODE_MODULES)|$(BUILD)|$(TARGET))/' --verbose
	$(call log_done,"Reformating worker slae sources completed.")


#
# Reformat e2e tests Python code
#
reformat_e2e_tests:
	$(call log_info,"Reformating e2e tests Python sources...")
	black $(E2E_TESTS_ROOT)  --exclude '/($(VENV_DIR)|$(NODE_MODULES)|$(BUILD)|$(TARGET))/' --verbose
	$(call log_done,"Reformating e2e tests Python sources completed.")


#
# Check bash code style
#
check_bash_code_style:
	$(call log_info,"Checking Bash code style...")
	find . ! -path "*$(NODE_MODULES)*" -type f \( -name "*.sh" -o -name "*.bats" \) | xargs --no-run-if-empty shellcheck
	$(call log_done,"Checking Bash code style completed.")

#
# Reformat bash shell scripts
#
reformat_bash:
	$(call log_info,"Reformating bash sources...")
	find . ! -path "*$(NODE_MODULES)*" -type f \( -name "*.sh" -o -name "*.bats" \) |xargs --no-run-if-empty shfmt  -i 2 -w
	$(call log_done,"Reformating bash shell scripts completed.")

#
# Run Python tests
#
test_python:
	$(call log_info,"Running Python unit tests...")
	PYTHONPATH=$(WORKER_RUNTIME_MAIN):$(WORKER_RUNTIME_TEST):$$PYTHONPATH pytest -v $(WORKER_RUNTIME_TEST)
	$(call log_done,"Python tests completed.")


#
# Run worker runtime tests with coverage
#
test_worker_runtime_coverage:
	$(call log_info,"Running worker runtime tests with coverage...")
	cd $(WORKER_RUNTIME_ROOT) && \
	  PYTHONPATH=src/main/python:src/test/python:$$PYTHONPATH \
	  python -m pytest src/test/python \
	    --cov=src/main/python/mdds_worker_runtime \
	    --cov-branch \
	    --cov-report=term-missing \
	    --cov-report=xml:target/python-coverage.xml
	$(call log_done,"Worker runtime tests with coverage completed.")


#
# Run e2e Python tests
#
test_e2e_python:
	$(call log_info,"Running e2e Python tests...")
	cd $(E2E_TESTS_ROOT) && \
	  PYTHONPATH=src/main/python:src/test/python:$$PYTHONPATH \
	  python -m pytest src/test/python
	$(call log_done,"e2e Python tests completed.")

#
# Run SLAE Python worker tests with coverage
#
test_worker_slae_coverage:
	$(call log_info,"Running SLAE Python worker tests with coverage...")
	cd $(WORKER_SLAE_ROOT) && \
	  PYTHONPATH=$(abspath $(WORKER_RUNTIME_ROOT)/src/main/python):src/main/python:src/test/python:$$PYTHONPATH \
	  python -m pytest src/test/python \
	    --cov=mdds_python_worker_solving_slae \
	    --cov-branch \
	    --cov-report=term-missing \
	    --cov-report=xml:target/python-coverage.xml
	$(call log_done,"SLAE Python worker tests with coverage completed.")


test_java_coverage:
	$(call log_info,"Running Java tests with coverage...")
	# Remove stale Java coverage reports.
	mkdir -p target
	rm -f target/java-coverage-files.txt
	find . \( -path "*/target/jacoco.exec" -o -path "*/target/site/jacoco/jacoco.xml" \) -type f -delete
	mvn verify
	find . -path "*/target/site/jacoco/jacoco.xml" -type f | sort | tee target/java-coverage-files.txt
	@test -s target/java-coverage-files.txt
	$(call log_done,"Java tests with coverage completed.")

#
# Reformat Java sources
#
reformat_java:
	$(call log_info,"Reformatting Java sources...")
	@find $(JAVA_ROOT) -name "*.java" | xargs java -jar $(GOOGLE_JAVA_FORMAT_HOME)/google-java-format.jar --replace
	$(call log_done,"Reformatting Java sources completed.")

#
# Reformat XML files (including pom.xml)
#
reformat_xml:
	$(call log_info,"Reformatting XML sources...")
	@find . -type f -name "*.xml" \
		-not -path "./$(NODE_MODULES)/*" \
		-not -path "./.idea/*" \
		-not -path "./*target/*" | while read -r file; do \
		echo "   → $$file"; \
		xmllint --format "$$file" --output "$$file"; \
	done
	$(call log_done,"Reformatting XML sources completed.")


#
# Wait until SonarQube is ready
#
wait_for_sonarqube:
	@retry_count=60; \
	current_retry=1; \
	$(call log_info_sh,"Waiting for SonarQube on $(SONAR_HOST_URL)..."); \
	while [ $$current_retry -le $$retry_count ]; do \
		STATUS=$$(curl -s "$(SONAR_HOST_URL)/api/system/status" | jq -r '.status // empty'); \
		if [ "$$STATUS" = "UP" ] || [ "$$STATUS" = "DEGRADED" ]; then \
			$(call log_done_sh,"SonarQube is ready: $$STATUS"); \
			exit 0; \
		else \
			$(call log_info_sh,"Attempt $$current_retry/$$retry_count: SonarQube status is '$$STATUS'. Retrying..."); \
			current_retry=$$((current_retry+1)); \
			sleep 5; \
		fi; \
	done; \
	$(call log_error_sh,"SonarQube did not become ready after $$retry_count attempts"); \
	exit 1


#
# Bootstrap SonarQube token for local development
#
bootstrap_sonar_token: wait_for_sonarqube
	$(call log_info,"Bootstrapping SonarQube token...")
	@SONAR_HOST_URL="$(SONAR_HOST_URL)" \
	  SONAR_ADMIN_LOGIN="$(SONAR_ADMIN_LOGIN)" \
	  SONAR_DEFAULT_ADMIN_PASSWORD="$(SONAR_DEFAULT_ADMIN_PASSWORD)" \
	  SONAR_ADMIN_PASSWORD="$(SONAR_ADMIN_PASSWORD)" \
	  SONAR_TOKEN_FILE="$(SONAR_TOKEN_FILE)" \
	  SONAR_TOKEN_NAME="$(SONAR_TOKEN_NAME)" \
	  bash mdds-dev/bootstrap-sonar-token.sh
	$(call log_done,"Bootstrapping SonarQube token completed.")


#
# Ensure SonarQube token exists and is valid
#
ensure_sonar_token: wait_for_sonarqube
	@TOKEN=""; \
	if [ -s "$(SONAR_TOKEN_FILE)" ]; then \
		TOKEN=$$(tr -d '\n' < "$(SONAR_TOKEN_FILE)"); \
	fi; \
	if [ -n "$$TOKEN" ] && curl -fsS -u "$$TOKEN:" "$(SONAR_HOST_URL)/api/authentication/validate" | jq -e '.valid == true' >/dev/null; then \
		$(call log_done_sh,"SonarQube token is valid."); \
	else \
		$(call log_info_sh,"SonarQube token is missing or invalid. Bootstrapping a new token..."); \
		SONAR_HOST_URL="$(SONAR_HOST_URL)" \
		SONAR_ADMIN_LOGIN="$(SONAR_ADMIN_LOGIN)" \
		SONAR_DEFAULT_ADMIN_PASSWORD="$(SONAR_DEFAULT_ADMIN_PASSWORD)" \
		SONAR_ADMIN_PASSWORD="$(SONAR_ADMIN_PASSWORD)" \
		SONAR_TOKEN_FILE="$(SONAR_TOKEN_FILE)" \
		SONAR_TOKEN_NAME="$(SONAR_TOKEN_NAME)" \
		bash mdds-dev/bootstrap-sonar-token.sh; \
	fi

#
# Run all tests with coverage
#
test_coverage: test_java_coverage \
	test_python_coverage \
	test_common_web_client_coverage

#
# Perform SonarQube scan and print results to console.
#
# This target does not run tests. It expects coverage reports to be generated beforehand by:
#   * java coverage
#   * python coverage
#   * common web client coverage
#
sonar_scan:
	@if [ -z "$$SONAR_TOKEN" ]; then \
		$(MAKE) ensure_sonar_token; \
	fi
	$(call log_info,"Running SonarQube analysis...")
	$(call log_info,"Java coverage report list expected at target/java-coverage-files.txt")
	$(call log_info,"Python coverage reports expected in worker target directories.")
	$(call log_info,"Common Web Client TypeScript coverage report expected at target directories")
	@test -s target/java-coverage-files.txt || { \
		$(call log_error_sh,"Java coverage report list is missing. Run 'make test_java_coverage' first."); \
		exit 1; \
	}
	@while read -r coverage_file; do \
		test -s "$$coverage_file" || { \
			$(call log_error_sh,"Java coverage report is missing: $$coverage_file"); \
			exit 1; \
		}; \
	done < target/java-coverage-files.txt
	@test -s $(WORKER_RUNTIME_ROOT)/target/python-coverage.xml || { \
		$(call log_error_sh,"Worker Runtime Python coverage report is missing. Run 'make test_python_coverage' first."); \
		exit 1; \
	}
	@test -s $(WORKER_SLAE_ROOT)/target/python-coverage.xml || { \
		$(call log_error_sh,"SLAE Worker Python coverage report is missing. Run 'make test_python_coverage' first."); \
		exit 1; \
	}
	@test -s $(COMMON_WEB_CLIENT_ROOT)/target/coverage/lcov.info || { \
		$(call log_error_sh,"Common Web Client TypeScript coverage report is missing. Run 'make test_common_web_client_coverage' first."); \
		exit 1; \
	}
	@# githubactions:S8544 is suppressed for build.yml because Sonar treats
	@# pip install -r as unlocked even though project requirements files pin
	@# all direct dependencies. A hash-based lock file is a separate future
	@# hardening task.
	@TOKEN="$${SONAR_TOKEN:-$$(tr -d '\n' < "$(SONAR_TOKEN_FILE)")}"; \
	SONAR_ORGANIZATION_ARG="" ; \
	if [ -n "$$SONAR_ORGANIZATION" ]; then \
		SONAR_ORGANIZATION_ARG="-Dsonar.organization=$$SONAR_ORGANIZATION" ; \
	fi ; \
	JAVA_COVERAGE_FILES=$$(paste -sd, target/java-coverage-files.txt); \
	PYTHON_COVERAGE_FILES="$(WORKER_RUNTIME_ROOT)/target/python-coverage.xml,$(WORKER_SLAE_ROOT)/target/python-coverage.xml"; \
	JS_COVERAGE_FILES="$(COMMON_WEB_CLIENT_ROOT)/target/coverage/lcov.info"; \
	mvn -B -ntp org.sonarsource.scanner.maven:sonar-maven-plugin:sonar \
	  -Dsonar.projectKey=$(SONAR_PROJECT_KEY) \
	  -Dsonar.host.url=$(SONAR_HOST_URL) \
	  $$SONAR_ORGANIZATION_ARG \
	  -Dsonar.coverage.jacoco.xmlReportPaths="$$JAVA_COVERAGE_FILES" \
	  -Dsonar.python.coverage.reportPaths="$$PYTHON_COVERAGE_FILES" \
	  -Dsonar.javascript.lcov.reportPaths="$$JS_COVERAGE_FILES" \
	  -Dsonar.exclusions='**/src/test/**,**/node_modules/**,**/target/**,**/build/**,**/dist/**,**/generated/**,**/__pycache__/**,**/.pytest_cache/**,**/.venv/**,**/venv/**' \
	  -Dsonar.test.inclusions='**/*Test.java,**/Test*.java,**/test_*.py,**/*_test.py,**/*.test.ts,**/*.test.tsx' \
	  -Dsonar.qualitygate.wait=$${SONAR_QUALITYGATE_WAIT:-false} \
	  -Dsonar.issue.ignore.multicriteria=e1 \
	  -Dsonar.issue.ignore.multicriteria.e1.ruleKey=githubactions:S8544 \
	  -Dsonar.issue.ignore.multicriteria.e1.resourceKey=.github/workflows/build.yml \
	  -Dsonar.maven.scanAll=true \
	  -Dsonar.coverage.exclusions='mdds-dto/src/main/java/com/mdds/dto/**/*.java' \
	  -Dsonar.token=$$TOKEN
	$(call log_info,"Checking SonarQube Quality Gate status...")
	@sleep 5
	$(call log_info,"Fetching total number of issues...");
	@TOKEN="$${SONAR_TOKEN:-$$(tr -d '\n' < "$(SONAR_TOKEN_FILE)")}" ; \
	curl -s -u $$TOKEN: \
	    "$(SONAR_HOST_URL)/api/issues/search?projectKeys=$(SONAR_PROJECT_KEY)&severities=INFO,MINOR,MAJOR,CRITICAL,BLOCKER&statuses=OPEN,CONFIRMED,REOPENED" \
	    | jq -r '" - Total issues: " + (.total|tostring)'
	$(call log_info,"Fetching list of issues...");
	@TOKEN="$${SONAR_TOKEN:-$$(tr -d '\n' < "$(SONAR_TOKEN_FILE)")}" ; \
	curl -s -u $$TOKEN: \
    	    "$(SONAR_HOST_URL)/api/issues/search?projectKeys=$(SONAR_PROJECT_KEY)&severities=INFO,MINOR,MAJOR,CRITICAL,BLOCKER&statuses=OPEN,CONFIRMED,REOPENED" \
    	    | jq -r '.issues[] | "- " + .severity + " | " + .component + ":" + (.line|tostring) + " → " + .message';
	$(call log_info,"Fetching additional metrics ...");
	@TOKEN="$${SONAR_TOKEN:-$$(tr -d '\n' < "$(SONAR_TOKEN_FILE)")}" ; \
	curl -s -u $$TOKEN: \
      "$(SONAR_HOST_URL)/api/measures/component?component=$(SONAR_PROJECT_KEY)&metricKeys=coverage,new_coverage,duplicated_lines_density,security_hotspots" \
      | jq -r '.component.measures[] | " - " + .metric + ": " + .value + "%"'
	@TOKEN="$${SONAR_TOKEN:-$$(tr -d '\n' < "$(SONAR_TOKEN_FILE)")}" ; \
	STATUS=$$(curl -s -u $$TOKEN: \
	  "$(SONAR_HOST_URL)/api/qualitygates/project_status?projectKey=$(SONAR_PROJECT_KEY)" \
	  | jq -r '.projectStatus.status'); \
	if [ "$$STATUS" = "ERROR" ]; then \
	  $(call log_error_sh, "Quality Gate failed!"); \
	  exit 1; \
	else \
	  $(call log_done_sh,"Quality Gate passed: $$STATUS"); \
	fi

#
# Run Java Tests
#
test_java:
	$(call log_info,"Running Java Unit Tests...")
	@if ! mvn clean test; then \
   		$(call log_error_sh, "Java Unit Tests failed"); \
   		exit 1; \
   	fi
	$(call log_done,"Running Java Unit Tests completed")

#
# Build Jars
#
build_jars:
	$(call log_info,"Building jars...")
	@if ! mvn clean install -DskipTests=true; then \
        $(call log_error_sh, "Building jars failed"); \
        exit 1; \
    fi
	$(call log_done,"Building jars completed")


#
# Package all Python workers
#
package_python_workers: package_python_worker_runtime package_python_worker_solving_slae


#
# Build Python Worker Runtime wheel package
#
package_python_worker_runtime:
	$(call log_info,"Building Python Worker Runtime wheel package...")
	@rm -rf \
	  "$(WORKER_RUNTIME_PACKAGE)/build" \
	  "$(WORKER_RUNTIME_DIST)" \
	  "$(WORKER_RUNTIME_PACKAGE)"/src/main/python/*.egg-info
	@cd "$(WORKER_RUNTIME_PACKAGE)" && \
	  python -m build \
	    --wheel \
	    --outdir "target/dist"
	@test -n "$$(find "$(WORKER_RUNTIME_DIST)" -maxdepth 1 -name '*.whl' -print -quit)"
	$(call log_done,"Python Worker Runtime wheel package was built successfully.")
	$(call log_info,"Built artifacts:")
	@ls -lh "$(WORKER_RUNTIME_DIST)"/*.whl


#
# Build Python Worker SLAE wheel package
#
package_python_worker_solving_slae:
	$(call log_info,"Building Python Worker solving SLAE wheel package...")
	@rm -rf \
	  "$(WORKER_SLAE_PACKAGE)/build" \
	  "$(WORKER_SLAE_DIST)" \
	  "$(WORKER_SLAE_PACKAGE)"/src/main/python/*.egg-info
	@cd "$(WORKER_SLAE_PACKAGE)" && \
	  python -m build \
	    --wheel \
	    --outdir "target/dist"
	@test -n "$$(find "$(WORKER_SLAE_DIST)" -maxdepth 1 -name '*.whl' -print -quit)"
	$(call log_done,"Python Worker solving SLAE wheel package was built successfully.")
	$(call log_info,"Built artifacts:")
	@ls -lh "$(WORKER_SLAE_DIST)"/*.whl



#
# Build jars without formatting or auto-fixing sources.
# Intended for CI and reproducible builds.
#
build_jars_ci:
	$(call log_info,"Building jars for CI...")
	@if ! mvn clean install -DskipTests=true; then \
        $(call log_error_sh, "Building jars for CI failed"); \
        exit 1; \
    fi
	$(call log_done,"Building jars for CI completed.")

#
# Start MDDS demo environment with all Docker containers
#
start_mdds_demo:
	$(call log_info,"Starting MDDS demo environment...")
	@DOCKER_GID="$(stat -c '%g' /var/run/docker.sock)" docker compose --progress=plain -f $(DEMO_HOME)/compose.demo.yml up -d --build --wait --wait-timeout 120
	$(call log_done,"Starting MDDS demo environment completed. MDDS environment is up!")

#
# Stop MDDS demo environment with all Docker containers
#
stop_mdds_demo:
	$(call log_info,"Stopping MDDS demo environment...")
	@docker compose --progress=plain -f $(DEMO_HOME)/compose.demo.yml down
	$(call log_done,"Stopping MDDS demo environment completed.")


#
# Start MDDS environment with all Docker containers
#
start_mdds_env:
	$(call log_info,"Starting MDDS environment...")
	@$(MAKE) create_config_file
	@$(E2E_TESTS_COMPOSE) up -d --wait --wait-timeout 120
	$(call log_done,"Starting MDDS environment completed. MDDS environment is up!")

#
# Stop MDDS environment with all Docker containers
#
stop_mdds_env:
	$(call log_info,"Stopping MDDS environment...")
	@$(E2E_TESTS_COMPOSE) down --volumes --remove-orphans
	$(call log_done,"Stopping MDDS environment completed.")

#
# Generate Postman environment-file with baseUrl
#
define FILE_CONTENT
{
  "name": "mdds-e2e-env",
  "values": [
    {
      "key": "baseUrl",
      "value": "http://$(MDDS_E2E_SERVER_HOST):$(MDDS_E2E_SERVER_PORT)",
      "enabled": true
    }
  ]
}
endef

create_config_file:
	$(call log_info,"Generating env.json file for newman...")
	$(file > $(E2E_TESTS_NEWMAN_HOME)/env.json,$(FILE_CONTENT))
	$(call log_done,"Generating env.json file for newman completed.")

#
# Run all e2e tests
#
test_e2e: test_e2e_newman test_e2e_python


#
# Start Docker containers and run Newman end to end smoke tests
#
test_e2e_newman:
	@set -e; \
	trap '$(MAKE) stop_mdds_env' EXIT; \
	$(MAKE) start_mdds_env; \
	$(call log_info_sh,"Running Newman end to end tests..."); \
	tar -C "$(E2E_TESTS_NEWMAN_HOME)" -cf - collection.json env.json | \
	  $(E2E_TESTS_COMPOSE) --profile e2e run --rm -T --no-deps newman; \
	$(call log_done_sh,"Newman end to end tests completed.")

#
# Setup python environment
#
setup_python_env: clear_python_env create_python_env install_python_libs

#
# Clear python environment
#
clear_python_env:
	$(call log_info,"Clearing python environment $(VENV_DIR)...")
	rm -Rf $(VENV_DIR)
	$(call log_done,"Clearing python environment $(VENV_DIR) completed.")

#
# Create python environment
#
create_python_env:
	$(call log_info,"Creating python environment $(VENV_DIR)...")
	mkdir -p $(VENV_DIR)
	$(PYTHON_BIN) -m venv $(VENV_DIR)
	$(call log_done,"Creating python environment $(VENV_DIR) completed.")

#
# Install python libraries
#
install_python_libs:
	$(call log_info,"Installing python libraries...")
	. $(VENV_DIR)/bin/activate; \
    python -m pip install --upgrade "pip>=25.3"; \
    python -m pip install -r requirements.txt --build-constraint $(PYTHON_BUILD_CONSTRAINTS)
	$(call log_done,"Installing python libraries competed.")


#
# Check that every file in project has license header
#
check_license:
	$(call log_info,"🔍 Checking license headers in source files...")
	@FILES=$$(find . \
		-type f \
		-not -path "*$(NODE_MODULES)*" \
		-not -path "./.idea/*" \
		-not -path "*/__pycache__/*" \
		-not -path "*/.pytest_cache/*" \
		-not -path "*/.ruff_cache/*" \
		-not -path "*/.git/*" \
		-not -path "./logs/*" \
		-not -path "./*target/*" \
		-not -path "*$(WEB_APP_DIR)/*" \
		-not -path "*.egg-info/*" \
		-not -name "*.csv" \
		-not -name "*.log" \
		-not -name "*.ico" \
		-not -name ".sonar_token" \
		-not -name "*.gitignore" \
		-not -name "*.prettierignore" \
		-not -name "*.gitkeep" \
		-not -name "*.dockerignore" \
		-not -name "*.json" \
		-not -name "*.env" \
		-not -name "requirements.lock.txt" \
		-not -name "uv.lock" \
		-not -name "*.coverage" \
		-not -name "*.iml" \
		-not -name "__init__.py"); \
	STATUS=0; \
	for f in $$FILES; do \
		if ! grep -q $(CHECK_LICENSE_STRING) $$f; then \
			$(call log_error_sh, "Missing license in: $$f"); \
			STATUS=1; \
		fi; \
	done; \
	if [ $$STATUS -eq 0 ]; then \
		$(call log_done_sh,"All files contain license header."); \
	else \
		$(call log_error_sh, "Some files are missing license header."); \
		exit 1; \
	fi

# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

PYTHON_ENV_HOME := ~/.venvs
PYTHON_BIN ?= python3.12
PYTHON_BUILD_CONSTRAINTS := deployment/python-base/build-constraints.txt
NODE_MODULES := node_modules
ROOT_PACKAGE_JSON := package.json
ROOT_PACKAGE_LOCK := package-lock.json
ROOT_NODE_MODULES_LOCK := $(NODE_MODULES)/.package-lock.json
MDDS_WEB_SERVER := mdds-web-server
MDDS_WEB_CLIENT := mdds-web-client
WEB_CLIENT_PACKAGE_JSON := $(MDDS_WEB_CLIENT)/package.json
WEB_CLIENT_PACKAGE_LOCK := $(MDDS_WEB_CLIENT)/package-lock.json
WEB_CLIENT_NODE_MODULES_LOCK := $(MDDS_WEB_CLIENT)/$(NODE_MODULES)/.package-lock.json
PYTHON_GENERATED_SOURCES := mdds_grpc_core/generated
PROJECT_ROOT := .
PROJECT_NAME := mdds
PROJECT_VERSION := 0.1.0
MDDS_PYTHON_WORKER_RUNTIME := mdds-python-worker-runtime
PYTHON_ROOT := $(PROJECT_ROOT)/$(MDDS_PYTHON_WORKER_RUNTIME)
PYTHON_MAIN := $(PYTHON_ROOT)/src/main/python
PYTHON_TEST := $(PYTHON_ROOT)/src/test/python
PYTHON_WORKER_RUNTIME_PACKAGE_DIR := $(MDDS_PYTHON_WORKER_RUNTIME)
PYTHON_WORKER_RUNTIME_DIST_DIR := $(PYTHON_WORKER_RUNTIME_PACKAGE_DIR)/target/dist
WEB_APP_DIR := $(PROJECT_ROOT)/$(MDDS_WEB_SERVER)/src/main/resources/static
TS_ROOT := src
JS_ROOT := src
JAVA_ROOT := $(PROJECT_ROOT)
VENV_DIR := $(PYTHON_ENV_HOME)/$(PROJECT_NAME)
USER_NAME := mddsproject
DEPLOYMENT_DIR := deployment
DEPLOYMENT_TEST_ROOT := $(PROJECT_ROOT)/$(DEPLOYMENT_DIR)/test
MDDS_E2E_SERVER_HOST ?= web-server
MDDS_E2E_SERVER_PORT ?= 8000
E2E_HOME := mdds-tests/e2e
E2E_PROJECT_NAME := mdds-e2e
E2E_COMPOSE := docker compose --project-name $(E2E_PROJECT_NAME) --progress=plain -f $(E2E_HOME)/docker-compose.yml
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
# Run server and tests with existing python env
#
run_all: reformat_and_check_all test_and_run

#
# Reformat and check all code
#
reformat_and_check_all: check_license build_and_push_main_images reformat_python check_python_code_style reformat_bash check_bash_code_style reformat_java reformat_xml sonar_scan

#
# Run tests and start server
#
test_and_run: test_all start_mdds_env

#
# Run all tests
#
test_all: test_python test_java test_e2e

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
# Build gRPC server Docker image for others
#
build_grpc_server_docker_image:
	$(call log_info,"Building gRPC server Docker image...")
	docker buildx build -f deployment/grpc-server/Dockerfile --progress=plain --tag $(USER_NAME)/grpc-server:$(PROJECT_VERSION) .
	$(call log_done,"Building gRPC server Docker image completed.")

#
# Push gRPC server Docker image
#
push_grpc_server_docker_image:
	$(call log_info,"Pushing gRPC server Docker image...")
	docker push $(USER_NAME)/grpc-server:$(PROJECT_VERSION)
	$(call log_done,"Pushing gRPC server Docker image completed.")

#
# Build Docker image for executor
#
build_executor_docker_image:
	$(call log_info,"Building Docker image for executor...")
	docker buildx build -f deployment/executor/Dockerfile --progress=plain --tag $(USER_NAME)/executor:$(PROJECT_VERSION) .
	$(call log_done,"Building Docker image for executor completed.")


#
# Push executor Docker image
#
push_executor_docker_image:
	$(call log_info,"Pushing executor Docker image...")
	docker push $(USER_NAME)/executor:$(PROJECT_VERSION)
	$(call log_done,"Pushing executor Docker image completed.")


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
# Build Docker image for result-consumer
#
build_result_consumer_docker_image:
	$(call log_info,"Building Docker image for result-consumer...")
	docker buildx build -f deployment/result-consumer/Dockerfile --progress=plain --tag $(USER_NAME)/result-consumer:$(PROJECT_VERSION) .
	$(call log_done,"Building Docker image for result-consumer completed.")

#
# Push result-consumer Docker image
#
push_result_consumer_docker_image:
	$(call log_info,"Pushing result-consumer Docker image...")
	docker push $(USER_NAME)/result-consumer:$(PROJECT_VERSION)
	$(call log_done,"Pushing result-consumer Docker image completed.")


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
	build_and_push_release_images


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
build_and_push_release_images: build_release_images push_release_images

#
# Build and push all observability Docker images
#
build_and_push_observability_images: \
		build_observability_images \
		push_observability_images

#
# Build main images. Here we do not build base Java and Python docker images since they are rarely changed.
#
build_main_images: build_jars build_grpc_server_docker_image build_executor_docker_image build_web_server_docker_image build_result_consumer_docker_image


#
# Build main Docker images without formatting or auto-fixing sources.
# Intended for CI and reproducible builds.
#
build_main_images_ci: build_jars_ci build_grpc_server_docker_image build_executor_docker_image build_web_server_docker_image build_result_consumer_docker_image


#
# Push main images.
#
push_main_images: push_grpc_server_docker_image push_executor_docker_image push_web_server_docker_image push_result_consumer_docker_image

#
# Build and push main images.
#
build_and_push_main_images: build_main_images push_main_images

#
# Build web-client and copy binaries to web-app folder of web-server
#
build_and_copy_web_client: reformat_ts check_js_code_style
	$(call log_info,"Building web-client and copying to web-app folder of web-server...")
	cd $(MDDS_WEB_CLIENT) && npm run build
	$(call log_done,"Building web-client and copying to web-app folder of web-server completed.")


#
# Build web-client without formatting or auto-fixing sources.
# Intended for CI and reproducible builds.
#
build_and_copy_web_client_ci: install_js_dependencies
	$(call log_info,"Building web-client for CI...")
	cd $(MDDS_WEB_CLIENT) && npm run build
	$(call log_done,"Building web-client for CI completed.")


#
# Reformat web client files
#
reformat_ts: install_js_dependencies
	$(call log_info,"Reformatting web client sources...")
	cd $(MDDS_WEB_CLIENT) && \
		./$(NODE_MODULES)/.bin/prettier --write $(TS_ROOT)/*
	$(call log_done,"Reformatting web client sources completed.")

#
# Check python code style
#
check_python_code_style:
	$(call log_info,"Checking python code style...")
	pycodestyle $(PYTHON_ROOT) --exclude=*$(VENV_DIR)*,*$(NODE_MODULES),*$(PYTHON_GENERATED_SOURCES)* --ignore=E501,W503
	ruff check $(PYTHON_ROOT) --fix --force-exclude --respect-gitignore
	PYTHONPATH=$(PYTHON_MAIN):$(PYTHON_TEST):$$PYTHONPATH pylint $(PYTHON_ROOT)/ --ignore $(VENV_DIR),$(PYTHON_GENERATED_SOURCES) --errors-only
	$(call log_done,"Checking python code style completed.")

#
# Check JavaScript code style
#
check_js_code_style: install_js_dependencies
	$(call log_info,"Checking JavaScript code style...")
	cd $(MDDS_WEB_CLIENT) && \
		../$(NODE_MODULES)/.bin/eslint \
			$(JS_ROOT) \
			--debug \
			--fix \
			--no-error-on-unmatched-pattern
	$(call log_done,"Checking JavaScript code style completed.")

#
# Reformat Python code
#
reformat_python:
	$(call log_info,"Reformating python sources...")
	black $(PYTHON_ROOT)  --exclude '/($(VENV_DIR)|$(NODE_MODULES)|$(PYTHON_GENERATED_SOURCES))/' --verbose
	$(call log_done,"Reformating python sources completed.")

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
	PYTHONPATH=$(PYTHON_MAIN):$(PYTHON_TEST):$$PYTHONPATH pytest -v $(PYTHON_TEST)
	$(call log_done,"Python tests completed.")


#
# Run Python tests with coverage
#
test_python_coverage:
	$(call log_info,"Running Python tests with coverage...")
	cd $(MDDS_PYTHON_WORKER_RUNTIME) && \
	  PYTHONPATH=src/main/python:src/test/python:$$PYTHONPATH \
	  python -m pytest src/test/python \
	    --cov=src/main/python/mdds_worker_runtime \
	    --cov-branch \
	    --cov-report=term-missing \
	    --cov-report=xml:target/python-coverage.xml
	$(call log_done,"Python tests with coverage completed.")


test_java_coverage:
	$(call log_info,"Running Java tests with coverage...")
	mvn clean verify
	mkdir -p target
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
# Perform Sonar Qube scan and put the results to console
#
sonar_scan: ensure_sonar_token
	$(call log_info,"Running SonarQube analysis...")
	$(call log_info,"Python coverage report expected at $(MDDS_PYTHON_WORKER_RUNTIME)/target/python-coverage.xml")
	@TOKEN=$$(tr -d '\n' < "$(SONAR_TOKEN_FILE)"); \
	mvn clean verify sonar:sonar \
	  -Dsonar.projectKey=$(SONAR_PROJECT_KEY) \
	  -Dsonar.host.url=$(SONAR_HOST_URL) \
	  -Dsonar.token=$$TOKEN
	$(call log_info,"Checking SonarQube Quality Gate status...")
	@sleep 5
	$(call log_info,"Fetching total number of issues...");
	@TOKEN=$$(tr -d '\n' < "$(SONAR_TOKEN_FILE)"); \
	curl -s -u $$TOKEN: \
	    "$(SONAR_HOST_URL)/api/issues/search?projectKeys=$(SONAR_PROJECT_KEY)&severities=INFO,MINOR,MAJOR,CRITICAL,BLOCKER&statuses=OPEN,CONFIRMED,REOPENED" \
	    | jq -r '" - Total issues: " + (.total|tostring)'
	$(call log_info,"Fetching list of issues...");
	@TOKEN=$$(tr -d '\n' < "$(SONAR_TOKEN_FILE)"); \
	curl -s -u $$TOKEN: \
    	    "$(SONAR_HOST_URL)/api/issues/search?projectKeys=$(SONAR_PROJECT_KEY)&severities=INFO,MINOR,MAJOR,CRITICAL,BLOCKER&statuses=OPEN,CONFIRMED,REOPENED" \
    	    | jq -r '.issues[] | "- " + .severity + " | " + .component + ":" + (.line|tostring) + " → " + .message';
	$(call log_info,"Fetching additional metrics ...");
	@TOKEN=$$(tr -d '\n' < "$(SONAR_TOKEN_FILE)"); \
	curl -s -u $$TOKEN: \
      "$(SONAR_HOST_URL)/api/measures/component?component=$(SONAR_PROJECT_KEY)&metricKeys=coverage,new_coverage,duplicated_lines_density,security_hotspots" \
      | jq -r '.component.measures[] | " - " + .metric + ": " + .value + "%"'
	@TOKEN=$$(tr -d '\n' < "$(SONAR_TOKEN_FILE)"); \
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
build_jars: build_and_copy_web_client
	$(call log_info,"Building jars...")
	@if ! mvn clean install -DskipTests=true; then \
        $(call log_error_sh, "Building jars failed"); \
        exit 1; \
    fi
	$(call log_done,"Building jars completed")


#
# Install root JavaScript dependencies
#
$(ROOT_NODE_MODULES_LOCK): $(ROOT_PACKAGE_JSON) $(ROOT_PACKAGE_LOCK)
	$(call log_info,"Installing root JavaScript dependencies...")
	npm ci --include=dev --no-audit --no-fund
	$(call log_done,"Root JavaScript dependencies installed.")

#
# Install web-client JavaScript dependencies
#
$(WEB_CLIENT_NODE_MODULES_LOCK): \
		$(WEB_CLIENT_PACKAGE_JSON) \
		$(WEB_CLIENT_PACKAGE_LOCK)
	$(call log_info,"Installing web-client JavaScript dependencies...")
	npm ci \
		--prefix $(MDDS_WEB_CLIENT) \
		--include=dev \
		--no-audit \
		--no-fund
	$(call log_done,"Web-client JavaScript dependencies installed.")

#
# Install all JavaScript dependencies
#
.PHONY: install_js_dependencies
install_js_dependencies: \
		$(ROOT_NODE_MODULES_LOCK) \
		$(WEB_CLIENT_NODE_MODULES_LOCK)

#
# Build Python Worker Runtime wheel package
#
package_python_worker_runtime:
	$(call log_info,"Building Python Worker Runtime wheel package...")
	@rm -rf \
	  "$(PYTHON_WORKER_RUNTIME_PACKAGE_DIR)/build" \
	  "$(PYTHON_WORKER_RUNTIME_DIST_DIR)" \
	  "$(PYTHON_WORKER_RUNTIME_PACKAGE_DIR)"/src/main/python/*.egg-info
	@cd "$(PYTHON_WORKER_RUNTIME_PACKAGE_DIR)" && \
	  python -m build \
	    --wheel \
	    --outdir "target/dist"
	@test -n "$$(find "$(PYTHON_WORKER_RUNTIME_DIST_DIR)" -maxdepth 1 -name '*.whl' -print -quit)"
	$(call log_done,"Python Worker Runtime wheel package was built successfully.")
	$(call log_info,"Built artifacts:")
	@ls -lh "$(PYTHON_WORKER_RUNTIME_DIST_DIR)"/*.whl


#
# Build jars without formatting or auto-fixing sources.
# Intended for CI and reproducible builds.
#
build_jars_ci: build_and_copy_web_client_ci
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
	@docker compose --progress=plain -f $(DEMO_HOME)/compose.demo.yml up -d --build --wait --wait-timeout 120
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
	@$(E2E_COMPOSE) up -d --wait --wait-timeout 120
	$(call log_done,"Starting MDDS environment completed. MDDS environment is up!")

#
# Stop MDDS environment with all Docker containers
#
stop_mdds_env:
	$(call log_info,"Stopping MDDS environment...")
	@$(E2E_COMPOSE) down --volumes --remove-orphans
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
	$(file > $(E2E_HOME)/env.json,$(FILE_CONTENT))
	$(call log_done,"Generating env.json file for newman completed.")


#
# Start Docker containers and run end to end tests
#
test_e2e:
	@set -e; \
	trap '$(MAKE) stop_mdds_env' EXIT; \
	$(MAKE) start_mdds_env; \
	$(call log_info_sh,"Running end to end tests..."); \
	tar -C "$(E2E_HOME)" -cf - collection.json env.json | \
	  $(E2E_COMPOSE) --profile e2e run --rm -T --no-deps newman; \
	$(call log_done_sh,"End to end tests completed.")

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
		-not -path "*/$(PYTHON_GENERATED_SOURCES)/*" \
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

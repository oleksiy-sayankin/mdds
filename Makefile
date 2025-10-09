# Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

PYTHON_ENV_HOME := ~/.venvs
NODE_MODULES := node_modules
PYTHON_GENERATED_SOURCES := mdds_grpc_core/generated
PROJECT_ROOT := .
PROJECT_NAME := mdds
MDDS_GRPC_CORE := mdds_grpc_core
PYTHON_ROOT := $(PROJECT_ROOT)/$(MDDS_GRPC_CORE)
JS_ROOT := $(PROJECT_ROOT)/$(MDDS_GRPC_CORE)/mdds_client
JAVA_ROOT := $(PROJECT_ROOT)
VENV_DIR := $(PYTHON_ENV_HOME)/$(PROJECT_NAME)
USER_NAME := mddsproject
DEPLOYMENT_DIR := deployment
DEPLOYMENT_TEST_ROOT := $(PROJECT_ROOT)/$(DEPLOYMENT_DIR)/test
MDDS_SERVER_PORT ?= 8000
MDDS_SERVER_HOST ?= localhost
E2E_HOME := tests/e2e
SONAR_HOST_URL ?= http://localhost:9000
SONAR_PROJECT_KEY ?= mdds
SONAR_TOKEN ?= $(shell cat .sonar_token)
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
reformat_and_check_all: check_license build_jars reformat_python check_python_code_style reformat_js check_js_code_style reformat_bash check_bash_code_style reformat_java reformat_xml sonar_scan

#
# Run tests and start server
#
test_and_run: test_all run_server

#
# Run all tests
#
test_all: test_python test_js test_java test_e2e

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
# Build base Docker that is use as root image for others
#
build_base_docker_image:
	$(call log_info,"Building base Docker image...")
	cp -a $(PYTHON_ROOT) $(DEPLOYMENT_TEST_ROOT)
	docker buildx build --progress=plain --tag $(USER_NAME)/$(PROJECT_NAME):base deployment/base
	rm -Rf $(DEPLOYMENT_TEST_ROOT)/$(PYTHON_ROOT)
	$(call log_done,"Building base Docker image completed.")

#
# Push base Docker image
#
push_base_docker_image:
	$(call log_info,"Pushing base Docker image...")
	docker push $(USER_NAME)/$(PROJECT_NAME):base
	$(call log_done,"Pushing base Docker image completed.")

#
# Build Docker image for Java tests
#
build_java_test_docker_image:
	$(call log_info,"Building Docker image for Java tests...")
	cp -a $(PYTHON_ROOT) $(DEPLOYMENT_TEST_ROOT)
	docker buildx build --progress=plain --tag $(USER_NAME)/$(PROJECT_NAME):test deployment/test
	rm -Rf $(DEPLOYMENT_TEST_ROOT)/$(PYTHON_ROOT)
	$(call log_done,"Building Docker image for Java tests completed.")

#
# Push Docker image for Java tests
#
push_java_test_docker_image:
	$(call log_info,"Pushing Docker image for Java tests...")
	docker push $(USER_NAME)/$(PROJECT_NAME):test
	$(call log_done,"Pushing Docker image for Java tests completed.")


#
# Build Docker image
#
build_image:
	$(call log_info,"Building Docker image...")
	docker buildx build --progress=plain --tag $(USER_NAME)/$(PROJECT_NAME):latest deployment
	$(call log_done,"Building Docker image completed.")


#
# Push Docker image
#
push_image:
	$(call log_info,"Pushing Docker image...")
	docker push $(USER_NAME)/$(PROJECT_NAME):latest
	$(call log_done,"Pushing Docker image completed.")

#
# Reformat JavaScript files
#
reformat_js:
	$(call log_info,"Reformating JavaScript sources...")
	prettier --write $(JS_ROOT)/
	$(call log_done,"Reformating JavaScript sources completed.")

#
# Check python code style
#
check_python_code_style:
	$(call log_info,"Checking python code style...")
	pycodestyle $(PYTHON_ROOT) --exclude=*$(VENV_DIR)*,*$(NODE_MODULES),*$(PYTHON_GENERATED_SOURCES)* --ignore=E501
	ruff check $(PYTHON_ROOT) --fix --force-exclude $(VENV_DIR) ./$(PYTHON_GENERATED_SOURCES)/ --respect-gitignore
	pylint $(PYTHON_ROOT)/ --ignore $(VENV_DIR),$(PYTHON_GENERATED_SOURCES) --errors-only
	$(call log_done,"Checking python code style completed.")

#
# Check JavaScript code style
#
check_js_code_style:
	$(call log_info,"Checking JavaScript code style...")
	eslint $(JS_ROOT) --debug --fix
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
	find . ! -path "*$(NODE_MODULES)*" -type f \( -name "*.sh" -o -name "*.bats" \) | xargs shellcheck
	$(call log_done,"Checking Bash code style completed.")

#
# Reformat bash shell scripts
#
reformat_bash:
	$(call log_info,"Reformating bash sources...")
	find . ! -path "*$(NODE_MODULES)*" -type f \( -name "*.sh" -o -name "*.bats" \) |xargs shfmt  -i 2 -w
	$(call log_done,"Reformating bash shell scripts completed.")

#
# Run Python tests
#
test_python:
	$(call log_info,"Running Python unit tests...")
	pytest -v
	$(call log_done,"Python tests completed.")

#
# Reformat Java sources
#
reformat_java:
	$(call log_info,"Reformatting Java sources...")
	@find $(JAVA_ROOT) -name "*.java" | xargs java -jar ~/google/google-java-format/google-java-format.jar --replace
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
# Perform Sonar Qube scan and put the results to console
#
sonar_scan:
	$(call log_info,"Running SonarQube analysis...")
	@mvn clean verify sonar:sonar \
	  -Dsonar.projectKey=$(SONAR_PROJECT_KEY) \
	  -Dsonar.host.url=$(SONAR_HOST_URL) \
	  -Dsonar.token=$(SONAR_TOKEN)
	$(call log_info,"Checking SonarQube Quality Gate status...")
	@sleep 5 # Wait for report is done by Sonar Qube Server
	$(call log_info,"Fetching total number of issues...");
	@curl -s -u $(SONAR_TOKEN): \
	    "$(SONAR_HOST_URL)/api/issues/search?projectKeys=$(SONAR_PROJECT_KEY)&severities=INFO,MINOR,MAJOR,CRITICAL,BLOCKER&statuses=OPEN,CONFIRMED,REOPENED" \
	    | jq -r '" - Total issues: " + (.total|tostring)'
	$(call log_info,"Fetching list of issues...");
	@curl -s -u $(SONAR_TOKEN): \
    	    "$(SONAR_HOST_URL)/api/issues/search?projectKeys=$(SONAR_PROJECT_KEY)&severities=INFO,MINOR,MAJOR,CRITICAL,BLOCKER&statuses=OPEN,CONFIRMED,REOPENED" \
    	    | jq -r '.issues[] | "- " + .severity + " | " + .component + ":" + (.line|tostring) + " → " + .message';
	$(call log_info,"Fetching additional metrics ...");
	@curl -s -u $(SONAR_TOKEN): \
      "$(SONAR_HOST_URL)/api/measures/component?component=$(SONAR_PROJECT_KEY)&metricKeys=coverage,duplicated_lines_density,security_hotspots" \
      | jq -r '.component.measures[] | " - " + .metric + ": " + .value + "%"'
	@STATUS=$$(curl -s -u $(SONAR_TOKEN): \
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
# Run JavaScript tests
#
test_js:
	$(call log_info,"Running JavaScript unit tests...")
	if ! npm test; then \
		$(call log_error_sh, "JS tests failed"); \
		exit 1; \
	fi
	$(call log_done,"JavaScript tests completed.")

#
# Run server
#
run_server:
	$(call log_info,"Starting web-server...")
	python -m run
	$(call log_done,"Starting web-server completed. Web-server is up!")

#
# Wait until server is ready by checking its health state
#
wait_for_server:
	@retry_count=10; \
	current_retry=1; \
	$(call log_info_sh,"Waiting for server on port $(MDDS_SERVER_PORT)..."); \
	while [ $$current_retry -le $$retry_count ]; do \
		if curl -s http://$(MDDS_SERVER_HOST):$(MDDS_SERVER_PORT)/health > /dev/null; then \
			$(call log_done_sh,"Server is up!"); \
			exit 0; \
		else \
			$(call log_info_sh,"Attempt $$current_retry/$$retry_count: Server is not ready yet. Retrying..."); \
			current_retry=$$((current_retry+1)); \
			sleep 2; \
		fi; \
	done; \
	$(call log_error_sh, "Server did not become ready after $$retry_count attempts"); \
	exit 1

#
# Start Docker container and run end to end tests
#
test_e2e:
	$(call log_info,"Starting up environment...")
	echo "MDDS_SERVER_PORT=$(MDDS_SERVER_PORT)" > $(E2E_HOME)/.env
	docker compose -f $(E2E_HOME)/docker-compose.yml up -d
	$(MAKE) wait_for_server
	$(call log_info,"Running end to end tests...")
	npm run test:e2e
	$(call log_info,"Shutting down environment...")
	docker compose -f $(E2E_HOME)/docker-compose.yml down
	$(call log_done,"End to end tests completed.")

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
	python3 -m venv $(VENV_DIR)
	$(call log_done,"Creating python environment $(VENV_DIR) completed.")

#
# Install python libraries
#
install_python_libs:
	$(call log_info,"Installing python libraries...")
	. $(VENV_DIR)/bin/activate; pip install -r requirements.txt
	$(call log_done,"Installing python libraries competed.")


#
# Check that every file in project has license header
#
check_license:
	$(call log_info,"🔍 Checking license headers in source files...")
	@FILES=$$(find . \
		-type f \
		-not -path "./$(NODE_MODULES)/*" \
		-not -path "./.idea/*" \
		-not -path "*/__pycache__/*" \
		-not -path "*/.pytest_cache/*" \
		-not -path "*/.ruff_cache/*" \
		-not -path "*/.git/*" \
		-not -path "./logs/*" \
		-not -path "./*target/*" \
		-not -path "*/$(PYTHON_GENERATED_SOURCES)/*" \
		-not -name "*.csv" \
		-not -name "*.log" \
		-not -name "*.ico" \
		-not -name ".sonar_token" \
		-not -name "*.gitignore" \
		-not -name "*.json" \
		-not -name "*.env" \
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

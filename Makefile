# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

all:
	make reformat
	make check_code_style
	make test
	make run

#
# Check code style
#
check_code_style:
	echo "[INFO] Check code style"
	pycodestyle slae_solver/ --exclude=mdds_env --ignore=E501
	ruff check slae_solver/ --fix --force-exclude mdds_env --respect-gitignore
	pylint slae_solver/ --ignore mdds_env/ --errors-only

#
# Reformat code
#
reformat:
	echo "[INFO] Reformat python sources"
	black .  --exclude mdds_env/ --verbose
#
# Run tests
#
test:
	echo "[INFO] Run unit tests"
	pytest -v

#
# Run server
#
run:
	python -m slae_solver.run

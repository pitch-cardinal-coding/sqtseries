# sqtseries Makefile
# Run, test, lint, and operate the embedded time-series database.
# Every day-to-day command in one place — see `make help`.

.PHONY: help \
	run run-config \
	install uninstall \
	test test-dafka test-docs test-resource-leaks test-concurrency test-async \
	lint lint-fix format \
	status clean preflight

# ============================================================================
# Shared Variables
# ============================================================================

PY     ?= /home/iam/devcode/.env/sqtseries/bin/python3
RUFF   := $(dir $(PY))ruff
CONFIG ?= config.toml
VENV   ?= /home/iam/devcode/.env/sqtseries

# Colors
GREEN  := \033[0;32m
YELLOW := \033[1;33m
RED    := \033[0;31m
NC     := \033[0m

# ============================================================================
# Help
# ============================================================================

help:
	@echo "$(GREEN)sqtseries Makefile$(NC)"
	@echo ""
	@echo "$(YELLOW)Run:$(NC)"
	@echo "  make run                      Start the service (foreground)"
	@echo "  make run-config               Start with $(CONFIG)"
	@echo ""
	@echo "$(YELLOW)Install:$(NC)"
	@echo "  make install                  Install sqtseries + dev dependencies into $(VENV)"
	@echo "  make uninstall                Remove sqtseries from the venv"
	@echo ""
	@echo "$(YELLOW)Test & lint:$(NC)"
	@echo "  make test                     Full test suite (excluding camera tests)"
	@echo "  make test-dafka               Adopted dafka/zyre pattern tests"
	@echo "  make test-docs                Documentation claims tests"
	@echo "  make test-resource-leaks      Resource leak / fd / thread tests"
	@echo "  make test-concurrency         Concurrent writer tests"
	@echo "  make test-async               Async cleanup tests"
	@echo "  make lint                     ruff check"
	@echo "  make lint-fix                 ruff check --fix"
	@echo "  make format                   ruff format + ruff check --fix"
	@echo ""
	@echo "$(YELLOW)Operate:$(NC)"
	@echo "  make status                   sqtseries processes"
	@echo "  make clean                    Wipe caches, __pycache__, .pytest_cache"
	@echo "  make preflight                Python version, venv, ruff versions"
	@echo ""

# ============================================================================
# Run
# ============================================================================

run:
	$(PY) -m sqtseries run

run-config:
	$(PY) -m sqtseries --config $(CONFIG) run

# ============================================================================
# Install
# ============================================================================

install:
	@echo "$(YELLOW)Installing sqtseries + dev dependencies into $(VENV)...$(NC)"
	$(VENV)/bin/python3 -m pip install -e ".[dev]"
	@echo "$(GREEN)Installed sqtseries$(NC)"

uninstall:
	$(VENV)/bin/python3 -m pip uninstall -y sqtseries

# ============================================================================
# Test & lint
# ============================================================================

test:
	$(PY) -m pytest tests/ -q --timeout=60 \
		--ignore=tests/test_camera_overlay_page.py \
		--ignore=tests/test_camera_feed.py

test-dafka:
	$(PY) -m pytest tests/test_dafka_adopted.py -v

test-docs:
	$(PY) -m pytest tests/test_docs_claims.py -v

test-resource-leaks:
	$(PY) -m pytest tests/test_resource_leaks.py -v

test-concurrency:
	$(PY) -m pytest tests/test_concurrency.py -v

test-async:
	$(PY) -m pytest tests/test_async_cleanup.py -v

lint:
	$(RUFF) check src tests scripts examples

lint-fix:
	$(RUFF) check --fix src tests scripts examples

format:
	$(RUFF) format src tests scripts examples
	$(RUFF) check --fix src tests scripts examples

# ============================================================================
# Operate
# ============================================================================

status:
	@echo "$(YELLOW)--- sqtseries processes ---$(NC)"
	@pgrep -af "python3.*sqtseries" || echo "  none running"

clean:
	@echo "$(YELLOW)Cleaning caches...$(NC)"
	@find . -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true
	@find . -name "*.pyc" -delete 2>/dev/null || true
	@rm -rf .pytest_cache .ruff_cache .hypothesis
	@rm -f .coverage
	@echo "$(GREEN)Clean$(NC)"

preflight:
	@echo "$(YELLOW)Python:$(NC)"
	@$(PY) --version
	@echo ""
	@echo "$(YELLOW)Ruff:$(NC)"
	@$(RUFF) --version 2>/dev/null || echo "  ruff not found"
	@echo ""
	@echo "$(YELLOW)Key packages:$(NC)"
	@$(PY) -c "import zmq; print(f'  pyzmq {zmq.__version__}')" 2>/dev/null || echo "  pyzmq not installed"
	@$(PY) -c "import fastapi; print(f'  fastapi {fastapi.__version__}')" 2>/dev/null || echo "  fastapi not installed"
	@$(PY) -c "import orjson; print(f'  orjson {orjson.__version__}')" 2>/dev/null || echo "  orjson not installed"
	@$(PY) -c "import structlog; print(f'  structlog {structlog.__version__}')" 2>/dev/null || echo "  structlog not installed"

# sqtseries Makefile
# Run, test, lint, and operate the embedded time-series database.
# Every day-to-day command in one place — see `make help`.

.PHONY: help \
	run run-config \
	install uninstall install-prod deploy deploy-no-build deploy-restart \
	test test-fast test-camera run-all coverage coverage-html \
	whl test-wheel \
	lint fix style \
	systemd-install systemd-uninstall systemd-status systemd-logs systemd-restart \
	status clean preflight

# ============================================================================
# Shared Variables
# ============================================================================

PY     ?= /home/iam/devcode/.env/sqtseries/bin/python3
RUFF   := $(dir $(PY))ruff
STYLE_RUFF := /home/iam/devcode/.env/sqtseries/bin/ruff
STYLE_DIRS := src tests scripts examples
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
	@echo "$(YELLOW)Install / deploy:$(NC)"
	@echo "  make install                  Install sqtseries + dev dependencies into $(VENV)"
	@echo "  make uninstall                Remove sqtseries from the venv"
	@echo "  make install-prod             Build + install to /opt/sqtseries (production)"
	@echo "  make deploy                   Build + install to /opt/sqtseries (same as install-prod)"
	@echo "  make deploy-no-build          Install existing dist/*.whl to /opt (skip build)"
	@echo "  make deploy-restart           Deploy + restart the systemd service"
	@echo ""
	@echo "$(YELLOW)Test & lint:$(NC)"
	@echo "  make test                     Full test suite (excluding camera tests)"
	@echo "  make test-fast                Fast subset (no stress/property/camera)"
	@echo "  make test-camera              Camera feed + overlay page (needs browsers)"
	@echo "  make run-all                  Per-file runner: timeouts + heartbeats + results (SUITE_TIMEOUT=, COVERAGE=1)"
	@echo "  make coverage                 Full suite under coverage + report (fail under 85%)"
	@echo "  make coverage-html            HTML report in /tmp/sqtseries_htmlcov"
	@echo "  make test-cache-health        Query cache + connection health tests"
	@echo "  make test-docs                Documentation claims tests"
	@echo "  make test-resource-leaks      Resource leak / fd / thread tests"
	@echo "  make test-concurrency         Concurrent writer tests"
	@echo "  make test-async               Async cleanup tests"
	@echo "  make lint                     ruff check + except-tuple guard (no fixes)"
	@echo "  make fix                      except-fix + ruff check --fix"
	@echo "  make style                    fix -> format -> format --check (mirrors the style() alias)"
	@echo "  make lint-fix                 ruff check --fix (legacy alias of fix)"
	@echo "  make format                   ruff format + ruff check --fix (legacy)"
	@echo ""
	@echo "$(YELLOW)Build:$(NC)"
	@echo "  make whl                      Build the release wheel into dist/"
	@echo "  make test-wheel               Isolated /tmp build+install+validate (gates dist/ copy)"
	@echo ""
	@echo "$(YELLOW)Operate:$(NC)"
	@echo "  make status                   sqtseries processes"
	@echo "  make systemd-install          Install + enable the systemd unit (user scope)"
	@echo "  make systemd-uninstall        Remove the systemd unit"
	@echo "  make systemd-status           Unit state"
	@echo "  make systemd-logs             Follow the unit journal"
	@echo "  make systemd-restart          Restart the unit"
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
# Prod install / deploy (stop → build → install to /opt/sqtseries)
# ============================================================================

install-prod: deploy

deploy:
	@echo "$(YELLOW)Deploying to production (/opt/sqtseries)...$(NC)"
	scripts/deploy.sh

deploy-no-build:
	@echo "$(YELLOW)Deploying to production (skip build)...$(NC)"
	scripts/deploy.sh --no-build

deploy-restart:
	@echo "$(YELLOW)Deploying to production + restart service...$(NC)"
	scripts/deploy.sh --restart

# ============================================================================
# Test & lint
# ============================================================================

test:
	$(PY) -m pytest tests/ -q --timeout=60 \
		--ignore=tests/test_camera_overlay_page.py \
		--ignore=tests/test_camera_feed.py

# Fast subset: pure unit + edge suites, no stress/property/camera/concurrency.
test-fast:
	$(PY) -m pytest tests/test_config.py tests/test_config_edge.py \
		tests/test_engine.py tests/test_db.py tests/test_query.py \
		tests/test_agg_edge.py tests/test_client.py tests/test_cli.py \
		tests/test_cli2.py tests/test_cli_edge.py tests/test_health.py \
		tests/test_ports.py tests/test_protocol.py tests/test_misc_edge.py \
		tests/test_logging.py -q --timeout=60

test-camera:
	$(PY) -m pytest tests/test_camera_feed.py tests/test_camera_overlay_page.py -q --timeout=120

# Per-file runner: hard per-suite timeout + 20s heartbeat + results/*.out.
# SUITE_TIMEOUT=600 make run-all | COVERAGE=1 make run-all | make run-all test_query.py
run-all:
	cd tests && ./run_all.sh

# Coverage: full suite measured, text report (fail under 85%), html on demand.
coverage:
	$(PY) -m pytest tests/ -q --timeout=60 \
		--ignore=tests/test_camera_overlay_page.py \
		--ignore=tests/test_camera_feed.py \
		--cov=sqtseries --cov-report=term --cov-fail-under=85
	rm -f .coverage.*

coverage-html: coverage
	$(PY) -m coverage html -d /tmp/sqtseries_htmlcov
	@echo "html report: /tmp/sqtseries_htmlcov"

test-cache-health:
	$(PY) -m pytest tests/test_cache_and_health.py -v

test-docs:
	$(PY) -m pytest tests/test_docs_claims.py -v

test-resource-leaks:
	$(PY) -m pytest tests/test_resource_leaks.py -v

test-concurrency:
	$(PY) -m pytest tests/test_concurrency.py -v

test-async:
	$(PY) -m pytest tests/test_async_cleanup.py -v

# Legacy aliases (kept for muscle memory; prefer lint/fix/style above).
lint-fix: fix

format:
	$(RUFF) format src tests scripts examples
	$(RUFF) check --fix src tests scripts examples

# ============================================================================
# Style pipeline (mirrors the style() shell alias over STYLE_DIRS)
# ============================================================================
# fix    except-fix + ruff check --fix (import sort via --extend-select I)
# lint   check-only pass over the same rules + the hard except guard
# style  full pipeline: fix -> format -> format --check
#
# Target version is py314 per pyproject.toml (the alias pins py313 because
# Cython needs parenthesized except tuples; sqtseries has no Cython, and
# `lint` below enforces the parenthesized form anyway).

lint:
	$(STYLE_RUFF) check --extend-select I $(STYLE_DIRS)
	@! grep -rnE 'except [A-Za-z_][A-Za-z_.]*, *[A-Za-z_.]' $(STYLE_DIRS) \
	  || (echo "ERROR: unparenthesized except tuple found — use 'except (A, B):'; see pyproject.toml" && exit 1)
	@echo "except-tuple check OK"

fix:
	@for f in $$(find $(STYLE_DIRS) -name '*.py' -type f); do \
		if grep -qE 'except\s+[A-Za-z_]\w*(\s*,\s*[A-Za-z_]\w*)+\s*:' "$$f"; then \
			sed -i -E 's/except\s+(\w+)\s*,\s*([^:]+)\s*:/except (\1, \2):/g' "$$f"; \
			echo "  fixed except syntax: $$f"; \
		fi; \
	done
	$(STYLE_RUFF) check --fix --extend-select I $(STYLE_DIRS)

style: fix
	$(STYLE_RUFF) format --target-version py314 $(STYLE_DIRS)
	$(STYLE_RUFF) format --check --target-version py314 $(STYLE_DIRS)
	@echo "$(GREEN)style: clean (ruff check + ruff format --check, whole project)$(NC)"

# ============================================================================
# Build (release wheel)
# ============================================================================

whl:
	@mkdir -p dist
	@echo "$(YELLOW)Building release wheel...$(NC)"
	scripts/build_wheel.sh dist

test-wheel:
	@echo "$(YELLOW)Building + installing + testing wheel in /tmp (isolated)...$(NC)"
	scripts/test_wheel.sh

# ============================================================================
# Operate
# ============================================================================

status:
	@echo "$(YELLOW)--- sqtseries processes ---$(NC)"
	@pgrep -af "python3.*sqtseries" || echo "  none running"

systemd-install:
	@echo "$(YELLOW)Installing systemd unit (user scope)...$(NC)"
	$(PY) -m sqtseries install

systemd-uninstall:
	@echo "$(YELLOW)Removing systemd unit...$(NC)"
	$(PY) -m sqtseries uninstall

systemd-status:
	@systemctl --user is-active sqtseries.service 2>/dev/null && systemctl --user status sqtseries.service --no-pager || systemctl status sqtseries.service --no-pager || true

systemd-logs:
	journalctl --user -u sqtseries.service -f || journalctl -u sqtseries.service -f || true

systemd-restart:
	systemctl --user restart sqtseries.service 2>/dev/null || sudo systemctl restart sqtseries.service

clean:
	@echo "$(YELLOW)Cleaning caches...$(NC)"
	@find . -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true
	@find . -name "*.pyc" -delete 2>/dev/null || true
	@rm -rf .pytest_cache .ruff_cache .hypothesis
	@rm -f .coverage .coverage.*
	@rm -rf tests/results tests/logs
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
	@$(PY) -c "import sqlite3; print(f'  sqlite {sqlite3.sqlite_version}')" 2>/dev/null || echo "  sqlite3 not available"
	@echo ""
	@echo "$(YELLOW)Disk (repo + tmp):$(NC)"
	@df -h . /tmp | awk 'NR==1 || /\/$$|\/tmp/'

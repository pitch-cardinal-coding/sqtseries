# sqtseries production installer (runs from dist/ with sudo)
# Installs the release wheel into /opt/sqtseries and manages the service.
# See HOW-TO-INSTALL.md for the one-page flow.

PREFIX   ?= /opt/sqtseries
PYTHON   ?= $(PREFIX)/bin/python3
CONFIG   ?= /opt/sqtseries/config.toml

GREEN  := \033[0;32m
YELLOW := \033[1;33m
RED    := \033[0;31m
NC     := \033[0m

help:
	@echo "$(GREEN)sqtseries production install$(NC)"
	@echo ""
	@echo "  sudo make install            venv + wheel + symlinks + config (no service start)"
	@echo "  sudo make systemd-install    write /etc/systemd/system/sqtseries.service, enable + start"
	@echo "  sudo make systemd-start      systemctl start sqtseries"
	@echo "  sudo make systemd-stop       systemctl stop sqtseries"
	@echo "  sudo make systemd-restart    systemctl restart sqtseries"
	@echo "  sudo make systemd-status     systemctl status sqtseries --no-pager"
	@echo "  sudo make systemd-logs       journalctl -u sqtseries -f"
	@echo "  sudo make systemd-uninstall  stop, disable, remove unit"
	@echo "  sudo make uninstall          remove /opt/sqtseries + symlinks (keeps the database)"
	@echo "  make preflight               versions, ports, service state"
	@echo ""
	@echo "  Variables: PREFIX=$(PREFIX) CONFIG=$(CONFIG)"

install:
	@echo "$(YELLOW)Installing sqtseries to $(PREFIX)...$(NC)"
	@if [ "$$(id -u)" != "0" ]; then echo "$(RED)Run as root: sudo make install$(NC)"; exit 1; fi
	mkdir -p $(PREFIX)
	python3 -m venv $(PREFIX)
	$(PREFIX)/bin/python3 -m pip install --upgrade pip 2>/dev/null | tail -1 || true
	$(PREFIX)/bin/python3 -m pip install -r requirements-prod.txt 2>&1 | tail -2
	$(PREFIX)/bin/python3 -m pip install --force-reinstall --no-deps sqtseries-*.whl 2>&1 | tail -2
	@if [ ! -f $(CONFIG) ]; then cp config.toml $(CONFIG); echo "  installed default $(CONFIG)"; fi
	ln -sf $(PREFIX)/bin/sqtseries /usr/local/bin/sqtseries
	@echo "$(GREEN)Installed. Next: sudo make systemd-install$(NC)"

uninstall:
	@echo "$(YELLOW)Uninstalling (database at ~/.sqtseries is kept)...$(NC)"
	-systemctl stop sqtseries 2>/dev/null || true
	-systemctl disable sqtseries 2>/dev/null || true
	rm -f /usr/local/bin/sqtseries
	rm -rf $(PREFIX)/bin $(PREFIX)/lib $(PREFIX)/lib64 $(PREFIX)/include $(PREFIX)/pyvenv.cfg
	@echo "$(GREEN)Uninstalled (config + database kept)$(NC)"

systemd-install:
	@echo "$(YELLOW)Installing system unit...$(NC)"
	@if [ "$$(id -u)" != "0" ]; then echo "$(RED)Run as root: sudo make systemd-install$(NC)"; exit 1; fi
	@if [ ! -f $(CONFIG) ]; then cp config.toml $(CONFIG); echo "  installed default $(CONFIG)"; fi
	$(PREFIX)/bin/python3 -m sqtseries --config $(CONFIG) install --system
	systemctl daemon-reload
	systemctl enable sqtseries
	# restart (not start): on upgrade the old process is still running the
	# previous venv, and plain start would leave it in place silently.
	systemctl restart sqtseries
	@echo "$(GREEN)Service active. Check: sudo make systemd-status$(NC)"

systemd-uninstall:
	-systemctl stop sqtseries 2>/dev/null || true
	-systemctl disable sqtseries 2>/dev/null || true
	rm -f /etc/systemd/system/sqtseries.service
	systemctl daemon-reload
	@echo "$(GREEN)Unit removed$(NC)"

systemd-start:
	systemctl start sqtseries

systemd-stop:
	systemctl stop sqtseries

systemd-restart:
	systemctl restart sqtseries

systemd-status:
	systemctl status sqtseries --no-pager || true

systemd-logs:
	journalctl -u sqtseries -f || true

preflight:
	@echo "$(YELLOW)--- versions ---$(NC)"
	@$(PREFIX)/bin/python3 -m sqtseries --version 2>/dev/null || echo "  not installed in $(PREFIX)"
	@$(PREFIX)/bin/python3 -c "import sqtseries; print('  '+sqtseries.__file__)" 2>/dev/null || true
	@echo "$(YELLOW)--- service ---$(NC)"
	@systemctl is-active sqtseries 2>/dev/null || echo "  inactive"
	@echo "$(YELLOW)--- symlinks ---$(NC)"
	@ls -la /usr/local/bin/sqtseries 2>/dev/null || echo "  no symlink"

# sqtseries — Production Install (dist/)

This directory is a self-contained release: `sqtseries-*.whl` +
`requirements-prod.txt` (minimal runtime deps) + `requirements.txt` (full
dev set) + `Makefile` (production installer for `/opt/sqtseries`) +
`config.toml` (default config template) + `docs/` (the full doc site) +
this file. Built and verified via `make test-wheel` (isolated `/tmp`
venv: build → install → functional validation → unit subset).

## What's in dist/

* `sqtseries-*.whl` — release wheel (pure Python, setuptools; entry point `sqtseries`)
* `requirements-prod.txt` — minimal prod deps (pinned; what `/opt` installs)
* `requirements.txt` — full dev/test set (pinned)
* `Makefile` — production installer for `/opt/sqtseries` (run with sudo)
* `config.toml` — default config template (copied to `/opt/sqtseries/config.toml` on install)
* `README.md` — project front page (also the target of `docs/systemd.html`'s README link)
* `docs/` — the documentation site (same pages as the repo `docs/`)
* `HOW-TO-INSTALL.md` — this file

## Quick install

```bash
cd dist
sudo make install           # venv + wheel + symlinks + default config
sudo make systemd-install   # system unit /etc/systemd/system/sqtseries.service, enable + start
```

Verify:

```bash
make preflight                        # versions, service state, symlink
/opt/sqtseries/bin/python3 -m sqtseries --version
/opt/sqtseries/bin/python3 -m sqtseries status
sqtseries health                      # via /usr/local/bin symlink
```

Write a reading and query it back:

```bash
curl -X POST http://127.0.0.1:12505/api/v1/write \
  -H "Content-Type: application/json" \
  -d '{"metric":"temp.outside","value":22.5,"tags":{"sensor":"garden"}}'
curl "http://127.0.0.1:12505/api/v1/read?metric=temp.outside"
```

## Operations

```bash
sudo make systemd-status    # is it active?
sudo make systemd-logs      # follow the journal
sudo make systemd-restart   # restart after a config change
sudo make systemd-stop      # stop (stays enabled)
sudo make systemd-uninstall # stop, disable, remove unit
sudo make uninstall         # remove /opt/sqtseries + symlinks (database kept)
```

Same operations without sudo from the repo (user scope):

```bash
make systemd-install        # sqtseries install (user unit)
make systemd-status
```

## Configuration

The service reads `/opt/sqtseries/config.toml` (installed from the
template on first install). Every setting is covered in
[docs/configuration.html](docs/configuration.html). After editing,
`sudo make systemd-restart`.

The database lives at `~/.sqtseries/data/db.sqlite` by default (override
with `[database] path`). Uninstall never touches the database or the
config — back them up by copying the files.

## Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 12501 | ZMQ PULL | Ingest |
| 12502 | ZMQ REP | Queries |
| 12503 | ZMQ XPUB | Live streaming |
| 12504 | ZMQ REP | Admin |
| 12505 | HTTP + WS | REST API + browser streaming |
| 12506 | ZMQ PUB | Connection events |

All bind `127.0.0.1`. Full details: [docs/index.html](docs/index.html).

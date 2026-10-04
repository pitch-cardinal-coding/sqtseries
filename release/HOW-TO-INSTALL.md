# sqtseries — Production Install (dist/)

This directory is a self-contained release: `sqtseries-*.whl` +
`requirements-prod.txt` (minimal runtime deps) + `requirements.txt` (full
dev set) + `Makefile` (production installer for `/opt/sqtseries`) +
`config.toml` (default config template) + `docs/` (the full doc site) +
this file.

Everything here except the wheel is generated. `make whl` writes this
directory from the sources in `release/`, `scripts/docs-build/` and
`requirements*.txt` — including this file and the `Makefile` beside it — so
nothing in it is edited by hand and nothing in it can fall behind the code.
Built and verified via `make test-wheel`, which builds the wheel in an
isolated `/tmp` venv, installs it with `--no-deps`, and runs the full test
suite against the installed artifact.

## What's in dist/

* `sqtseries-*.whl` — release wheel (pure Python, setuptools; entry point `sqtseries`)
* `requirements-prod.txt` — minimal prod deps (pinned; what `/opt` installs)
* `requirements.txt` — full dev/test set (pinned)
* `Makefile` — production installer for `/opt/sqtseries` (run with sudo)
* `config.toml` — default config template (copied to `/opt/sqtseries/config.toml` on install)
* `README.md` — project front page (also the target of `docs/systemd.html`'s README link)
* `docs/` — the documentation site (served by the running service at `/docs`)
* `HOW-TO-INSTALL.md` — this file

## Prerequisites

Python 3.14 or newer, and a working `venv`:

```bash
sudo apt install python3-venv libjemalloc2 -y
```

`libjemalloc2` is recommended, not required: when present, `sqtseries install`
preloads the jemalloc allocator in the service unit (it returns freed memory
faster under sustained load). Without it the service runs correctly on the
default allocator.

`python3-venv` is only needed on distributions that ship `ensurepip`
separately — Debian and Ubuntu before 26.04 do, and `python3 -m venv` fails
with "ensurepip is not available" without it. On a distribution that bundles
it the install already works and the package is simply not found.

## Quick install

```bash
cd dist
sudo make install           # venv + wheel + symlinks + default config
sudo make systemd-install   # system unit /etc/systemd/system/sqtseries.service, enable + start
```

Verify (all through the `/usr/local/bin` symlink):

```bash
make preflight                        # versions, service state, symlink
sqtseries --version
sqtseries status
sqtseries health                      # database integrity probe
```

Write a reading and query it back:

```bash
curl -X POST http://127.0.0.1:12505/api/v1/write \
  -H "Content-Type: application/json" \
  -d '{"metric":"temp.outside","value":22.5,"tags":{"sensor":"garden"}}'
curl "http://127.0.0.1:12505/api/v1/read?metric=temp.outside"
```

## Upgrading

Replace the wheel in this directory with the newer one and run the same two
commands. There is no separate upgrade step and no migration to run: the
database is a plain SQLite file that the new code opens in place.

```bash
cp /path/to/new/sqtseries-*.whl .
sudo make install           # replaces the package in the existing venv
sudo make systemd-install   # rewrites the unit and restarts onto the new code
```

Two things worth knowing:

- **`make install` alone does not change the running code.** It rewrites the
  files under the live service's venv; the already-running process keeps
  serving the old code until it restarts. That is why `systemd-install`
  restarts rather than starts, and why both commands are needed.
- **Your data and config are not touched.** The venv is replaced in place and
  `/opt/sqtseries/config.toml` is only written when it does not already exist.
  `sudo make uninstall` likewise keeps both.

Running `sudo make install` twice in a row is safe.

## Uninstalling

```bash
sudo make systemd-uninstall   # stop, disable, remove the unit
sudo make uninstall           # remove the venv + the /usr/local/bin symlink
```

That leaves `/opt/sqtseries/config.toml` and `~/.sqtseries/` in place — the
configuration and the database. Copy them somewhere safe before you delete
them, or the measurements go with them.

## Operations

```bash
sudo make systemd-status    # is it active?
sudo make systemd-logs      # follow the journal
sudo make systemd-restart   # restart after a config change
sudo make systemd-stop      # stop (stays enabled)
sudo make systemd-uninstall # stop, disable, remove unit
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
| 12500 | ZMQ PULL | Ingest (auto-detected — see below) |
| 12502 | ZMQ REP | Queries |
| 12503 | ZMQ XPUB | Live streaming |
| 12504 | ZMQ REP | Admin |
| 12505 | HTTP + WS | REST API + browser streaming |
| 12506 | ZMQ PUB | Connection events |

Only the ingest port moves. `[ingestion] port = 12501` is the configured
value, but `[ports] auto_detect` is `true` by default, so the service binds the
first free port in `port_range_start..port_range_end` (12500–12700) — normally
**12500**, and `12501` is used only when auto-detect is off. `sqtseries status`
always prints the ports actually bound, and the same values are written to
`runtime.json` next to the database; trust that over any table.

The five ZeroMQ ports bind `127.0.0.1` only. **The HTTP gateway binds
`0.0.0.0` by default and has no authentication** — anything that can reach this
host on port 12505 can read and write the database. Set `http.host = "127.0.0.1"`
in the config to restrict it, or front it with a proxy that authenticates.
Full details: [docs/index.html](docs/index.html).

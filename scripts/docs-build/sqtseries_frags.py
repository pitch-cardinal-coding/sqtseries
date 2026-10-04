"""Shared tables for the sqtseries pages.

Every row is transcribed from the source of truth, never from memory:

* CLI options come from ``src/sqtseries/cli.py`` and ``sqtseries --help``.
* Settings come from ``src/sqtseries/config.py`` (each field's own default).
* Routes come from ``src/sqtseries/gateway/{app,routes}.py``.
* Aggregations come from ``AGGREGATORS`` in ``src/sqtseries/query/agg.py``.
* Admin commands come from ``Service._admin_handler`` in ``service.py``.

The point of sharing them is that one flag, key, route or aggregate cannot be
described two different ways on two different pages. ``verify_docs.py`` proves
the transcription still matches the code.
"""

import pathlib

FRAGS: dict[str, str] = {}

# ------------------------------------------------------------ CLI commands ---

FRAGS["cli_table"] = r"""
  <table class="opts">
    <thead><tr><th>Command</th><th>What it does</th><th>Needs the service?</th></tr></thead>
    <tbody>
      <tr><td class="opt">run</td>
        <td>Start the service in the foreground: opens the database, binds all
        six ports, starts the background tasks, prints where the dashboard,
        docs and health check live, then waits for SIGINT or SIGTERM.</td>
        <td class="muted">no — it <em>is</em> the service</td></tr>
      <tr><td class="opt">stop</td>
        <td>Send SIGTERM to the pid in <code>runtime.json</code>, then wait up
        to 10 s for it to actually exit. Refuses to stop an instance whose
        database is not the one this invocation resolves.</td>
        <td>yes</td></tr>
      <tr><td class="opt">status</td>
        <td>Print running/not-running, the pid, version, database path, the
        <strong>real</strong> bound ports, and the links.</td>
        <td>no</td></tr>
      <tr><td class="opt">ports</td>
        <td>Print the active ports. With no service running, prints the
        <em>configured</em> ports and says so.</td>
        <td>no</td></tr>
      <tr><td class="opt">health</td>
        <td>Run <code>PRAGMA quick_check</code> against the database. Prints
        <code>ok</code> and exits 0, or <code>degraded: …</code> and exits 1.</td>
        <td>no</td></tr>
      <tr><td class="opt">stats</td>
        <td>Print the number of metrics and the number of series. Migrates the
        file first, so it works on a brand-new database.</td>
        <td>no</td></tr>
      <tr><td class="opt">backup</td>
        <td>Write a consistent snapshot with <code>VACUUM INTO</code> into
        <code>backup.path</code>, named <code>sqtseries-YYYYmmdd-HHMMSS.db</code>.</td>
        <td>no — safe to run against a live service</td></tr>
      <tr><td class="opt">optimize</td>
        <td>Run <code>PRAGMA optimize</code> to refresh query-planner
        statistics.</td>
        <td>no</td></tr>
      <tr><td class="opt">vacuum</td>
        <td>Run a full <code>VACUUM</code> to shrink the file and rebuild the
        free pages. Cannot run inside a transaction, so stop the service first
        for anything large.</td>
        <td>recommended off</td></tr>
      <tr><td class="opt">install</td>
        <td>Generate and install the systemd unit.</td>
        <td>no</td></tr>
      <tr><td class="opt">uninstall</td>
        <td>Remove the systemd unit.</td>
        <td>no</td></tr>
    </tbody>
  </table>
  <p class="small muted">Global options belong <strong>before</strong> the
  subcommand: <code class="nb">sqtseries --config c.toml run</code>, never
  <code class="nb">sqtseries run --config c.toml</code>.</p>
"""

FRAGS["cli_globals"] = r"""
  <table class="opts">
    <thead><tr><th>Option</th><th>Argument</th><th>Default</th><th>What it does</th></tr></thead>
    <tbody>
      <tr><td class="opt">--config</td><td>PATH</td><td class="def">none</td>
        <td>Settings file to load. <code>.toml</code>, <code>.yaml</code>,
        <code>.yml</code> or <code>.json</code>, chosen by suffix. A missing
        file is an error, not a silent fallback to defaults. The same value can
        come from <code>SQT_SERIES_CONFIG_FILE</code>.</td></tr>
      <tr><td class="opt">--db</td><td>PATH</td><td class="def">none</td>
        <td>Database file to use, overriding both the config file and
        <code>SQT_SERIES_DATABASE__PATH</code>. The value is the
        <em>last</em> override applied, so it wins over everything.</td></tr>
      <tr><td class="opt">--version</td><td class="muted">none</td>
        <td class="def">off</td>
        <td>Print the version and exit.</td></tr>
      <tr><td class="opt">--help</td><td class="muted">none</td>
        <td class="def">off</td>
        <td>Print the options for the group or the subcommand and exit.</td></tr>
    </tbody>
  </table>
  <p class="small muted">Two of the subcommands take one flag of their own:
  <code>install --system</code> and <code>uninstall --system</code> install to
  <code>/etc/systemd/system/</code> instead of the per-user
  <code>~/.config/systemd/user/</code>.</p>
"""

# ------------------------------------------------------------------ ports ---

FRAGS["ports_table"] = r"""
  <table class="ports">
    <thead><tr><th>Port</th><th>Socket</th><th>Binds</th><th>Carries</th></tr></thead>
    <tbody>
      <tr><td class="opt">12500+</td><td>auto-detected</td><td>127.0.0.1</td>
        <td><strong>Ingest.</strong> The first free port in
        <code>port_range_start…port_range_end</code>, not the configured
        <code>ingestion.port</code>. Read the real one from
        <code>sqtseries status</code> or <code>runtime.json</code>.</td></tr>
      <tr><td class="opt">12502</td><td>ROUTER</td><td>127.0.0.1</td>
        <td><strong>Query.</strong> One request frame in, one reply frame out.
        Wire-compatible with a plain REQ client.</td></tr>
      <tr><td class="opt">12503</td><td>XPUB</td><td>127.0.0.1</td>
        <td><strong>Stream.</strong> Two-part frames, <code>[topic, json]</code>.
        XPUB, not PUB, so subscribe and unsubscribe events arrive on the wire
        and subscriber counts are exact.</td></tr>
      <tr><td class="opt">12504</td><td>REP</td><td>127.0.0.1</td>
        <td><strong>Admin.</strong> <code>{"cmd": …}</code> in, a status
        envelope out.</td></tr>
      <tr><td class="opt">12505</td><td>HTTP + WebSocket</td><td><code>http.host</code></td>
        <td><strong>HTTP.</strong> The REST API, the three WebSocket streams,
        the admin dashboard, these docs, and the self-hosted Swagger/ReDoc.
        Binds <code>0.0.0.0</code> by default, so a LAN machine can reach it.</td></tr>
      <tr><td class="opt">12506</td><td>PUB</td><td>127.0.0.1</td>
        <td><strong>Stats.</strong> Connect, disconnect and subscription
        events, plus a periodic <code>report</code>. Two-part frames like the
        stream socket.</td></tr>
    </tbody>
  </table>
  <div class="box warn">
    <span class="lbl">The ingest port is not the one you configured</span>
    <p>With <code>ports.auto_detect = true</code> (the default) the ingest port
    is the <strong>first free port in the range</strong>, which is normally
    12500 — not <code>ingestion.port</code>, which is 12501. A client hard-coded
    to 12501 works only when <code>auto_detect = false</code> or when 12500 is
    already taken. Always read the real ports from
    <code>sqtseries status</code>, or pass them from the same source your
    launcher used. The five fixed ports are reserved before the detector runs,
    so it can never hand out one of them.</p>
  </div>
"""

# ---------------------------------------------------------------- settings ---

FRAGS["settings_database"] = r"""
  <table class="opts">
    <thead><tr><th>Key</th><th>Environment variable</th><th>Default</th><th>What it does</th></tr></thead>
    <tbody>
      <tr><td class="opt">database.path</td><td class="opt">SQT_SERIES_DATABASE__PATH</td>
        <td class="def">~/.sqtseries/data/db.sqlite</td>
        <td>The one SQLite file that holds everything. Parent directories are
        created. <code>~</code> is expanded.</td></tr>
      <tr><td class="opt">database.page_size</td><td class="opt">SQT_SERIES_DATABASE__PAGE_SIZE</td>
        <td class="def">8192</td>
        <td>SQLite page size in bytes. Applied <strong>only when the file is
        created</strong> — SQLite cannot change it afterwards. It must be set
        before WAL, so the bootstrap connection skips WAL.</td></tr>
      <tr><td class="opt">database.cache_size</td><td class="opt">SQT_SERIES_DATABASE__CACHE_SIZE</td>
        <td class="def">-64000</td>
        <td>Page cache per connection, in KiB when negative. 8 MB on the
        shipped code path — see the note below on why this default is smaller
        than it looks.</td></tr>
      <tr><td class="opt">database.mmap_size</td><td class="opt">SQT_SERIES_DATABASE__MMAP_SIZE</td>
        <td class="def">268435456</td>
        <td>Bytes of the file mapped into the address space (256 MiB). Reading
        through the mapping skips a copy from the page cache.</td></tr>
      <tr><td class="opt">database.busy_timeout</td><td class="opt">SQT_SERIES_DATABASE__BUSY_TIMEOUT</td>
        <td class="def">5000</td>
        <td>Milliseconds SQLite waits on a locked database before returning
        <code>SQLITE_BUSY</code>.</td></tr>
      <tr><td class="opt">database.journal_size_limit</td><td class="opt">SQT_SERIES_DATABASE__JOURNAL_SIZE_LIMIT</td>
        <td class="def">67108864</td>
        <td>Bytes the WAL may keep after a checkpoint (64 MiB).</td></tr>
      <tr><td class="opt">database.threads</td><td class="opt">SQT_SERIES_DATABASE__THREADS</td>
        <td class="def">4</td>
        <td>SQLite worker threads.</td></tr>
      <tr><td class="opt">database.batch_size</td><td class="opt">SQT_SERIES_DATABASE__BATCH_SIZE</td>
        <td class="def">2000</td>
        <td>Points per insert transaction. Must be <strong>1…8191</strong>: above
        8191 the batch can exceed <code>SQLITE_MAX_VARIABLE_NUMBER</code> and a
        valid batch starts failing. A burst commits in one
        <code>BEGIN IMMEDIATE</code>, so a batch is all-or-nothing.</td></tr>
      <tr><td class="opt">database.flush_interval</td><td class="opt">SQT_SERIES_DATABASE__FLUSH_INTERVAL</td>
        <td class="def">1.0</td>
        <td>Seconds.</td></tr>
    </tbody>
  </table>
"""

FRAGS["settings_ports"] = r"""
  <table class="opts">
    <thead><tr><th>Key</th><th>Environment variable</th><th>Default</th><th>What it does</th></tr></thead>
    <tbody>
      <tr><td class="opt">ingestion.port</td><td class="opt">SQT_SERIES_INGESTION__PORT</td>
        <td class="def">12501</td>
        <td>Configured ingest port. Used as-is only when
        <code>ports.auto_detect = false</code>.</td></tr>
      <tr><td class="opt">ingestion.hwm</td><td class="opt">SQT_SERIES_INGESTION__HWM</td>
        <td class="def">101000</td>
        <td>Receive high-water mark on the PULL socket. Frames buffer up to
        this before senders block, so a producer burst is absorbed rather than
        refused. Must be ≥ 1.</td></tr>
      <tr><td class="opt">ingestion.pending_max</td><td class="opt">SQT_SERIES_INGESTION__PENDING_MAX</td>
        <td class="def">100</td>
        <td>Batches outstanding between the receive loop and the persister.
        This is the backpressure point: at 100, the receiver stops reading and
        the socket's HWM takes over. Must be ≥ 1.</td></tr>
      <tr><td class="opt">ingestion.max_message_size</td><td class="opt">SQT_SERIES_INGESTION__MAX_MESSAGE_SIZE</td>
        <td class="def">52428800</td>
        <td>Largest accepted frame in bytes (50 MiB). A larger frame is
        refused by the socket, not parsed.</td></tr>
      <tr><td class="opt">ingestion.reject_client_timestamp_skew_s</td><td class="opt">SQT_SERIES_INGESTION__REJECT_CLIENT_TIMESTAMP_SKEW_S</td>
        <td class="def">300.0</td>
        <td>Seconds a client timestamp may differ from the server clock
        before the write is refused (400 over HTTP, counted invalid over ZMQ).
        Set <code>0</code> to accept any timestamp. This also sets how long an
        hour must be finished before the rollup will touch it, so a write can
        never land in an hour already summarised.</td></tr>
      <tr><td class="opt">query.port</td><td class="opt">SQT_SERIES_QUERY__PORT</td>
        <td class="def">12502</td>
        <td>Query socket port.</td></tr>
      <tr><td class="opt">query.timeout_s</td><td class="opt">SQT_SERIES_QUERY__TIMEOUT_S</td>
        <td class="def">30.0</td>
        <td>Seconds one query may run. Over HTTP the client gets
        <code>504</code>; over ZMQ it gets <code>QUERY_TIMEOUT</code>. Also
        bounds one HTTP read. <code>None</code> or ≤ 0 disables the cap — the
        query still runs off the event loop.</td></tr>
      <tr><td class="opt">query.max_rows</td><td class="opt">SQT_SERIES_QUERY__MAX_ROWS</td>
        <td class="def">10000</td>
        <td>Hard cap on raw rows one query may materialise. Exceeding it is an
        error, never a silent truncation: <code>413</code> over HTTP,
        <code>MAX_ROWS_EXCEEDED</code> over ZMQ. <code>0</code> means unbounded.
        The check is a cheap <code>COUNT(*)</code> before the fetch, so an
        over-cap query never pays for the rows.</td></tr>
      <tr><td class="opt">query.max_inflight</td><td class="opt">SQT_SERIES_QUERY__MAX_INFLIGHT</td>
        <td class="def">64</td>
        <td>Concurrent query dispatches. Past it, requests stay in the socket
        pipe (HWM-bounded) and are counted in <code>queries_shed</code> rather
        than spawning another handler. Must be ≥ 1.</td></tr>
      <tr><td class="opt">streaming.port</td><td class="opt">SQT_SERIES_STREAMING__PORT</td>
        <td class="def">12503</td>
        <td>XPUB stream socket port.</td></tr>
      <tr><td class="opt">streaming.linger_seconds</td><td class="opt">SQT_SERIES_STREAMING__LINGER_SECONDS</td>
        <td class="def">30.0</td>
        <td>Seconds a just-unsubscribed topic is remembered as "lingering"
        before it stops being reported. Keeps a flapping subscriber from
        flickering in the topic list.</td></tr>
      <tr><td class="opt">streaming.topic_totals_max</td><td class="opt">SQT_SERIES_STREAMING__TOPIC_TOTALS_MAX</td>
        <td class="def">10000</td>
        <td>How many recent topics keep a running publish total in memory. The
        counter is updated on the ingest path, so a producer using a new metric
        name per point would otherwise grow it without limit. The oldest topic
        is dropped past this cap; it still appears in the Topics panel (which
        unions this counter with the stored metrics) with its total shown as
        unknown. Must be ≥ 1.</td></tr>
      <tr><td class="opt">admin.port</td><td class="opt">SQT_SERIES_ADMIN__PORT</td>
        <td class="def">12504</td>
        <td>Admin socket port.</td></tr>
      <tr><td class="opt">stats.enabled</td><td class="opt">SQT_SERIES_STATS__ENABLED</td>
        <td class="def">true</td>
        <td>Run the stats publisher. Off means port 12506 is never bound.</td></tr>
      <tr><td class="opt">stats.port</td><td class="opt">SQT_SERIES_STATS__PORT</td>
        <td class="def">12506</td>
        <td>Stats PUB socket port.</td></tr>
      <tr><td class="opt">http.port</td><td class="opt">SQT_SERIES_HTTP__PORT</td>
        <td class="def">12505</td>
        <td>HTTP and WebSocket port.</td></tr>
      <tr><td class="opt">http.host</td><td class="opt">SQT_SERIES_HTTP__HOST</td>
        <td class="def">0.0.0.0</td>
        <td>Interface the HTTP gateway binds. The default is every interface,
        so a phone on the same network can open the dashboard. Set
        <code>127.0.0.1</code> for local-only. The five ZMQ ports stay
        loopback-only whatever this is set to.</td></tr>
      <tr><td class="opt">http.cors_origins</td><td class="opt">SQT_SERIES_HTTP__CORS_ORIGINS</td>
        <td class="def">["*"]</td>
        <td>Allowed browser origins. A list in TOML. There is no
        authentication, so <code>*</code> plus a non-loopback
        <code>http.host</code> means anyone who can reach the port can read and
        write. Narrow both on an untrusted network.</td></tr>
      <tr><td class="opt">http.rate_limit_per_minute</td><td class="opt">SQT_SERIES_HTTP__RATE_LIMIT_PER_MINUTE</td>
        <td class="def">600</td>
        <td>Requests per minute per peer address, on a fixed one-minute window.
        Over the limit is <code>429</code>. Must be ≥ 1. Keyed on the socket peer
        address; <code>X-Forwarded-For</code> is deliberately not trusted.</td></tr>
      <tr><td class="opt">http.max_websocket_connections</td><td class="opt">SQT_SERIES_HTTP__MAX_WEBSOCKET_CONNECTIONS</td>
        <td class="def">1000</td>
        <td>Total live WebSocket budget across all three endpoints. A new
        connection past the cap is closed with code <code>1013</code>. Must be
        ≥ 1.</td></tr>
      <tr><td class="opt">ports.port_range_start</td><td class="opt">SQT_SERIES_PORTS__PORT_RANGE_START</td>
        <td class="def">12500</td>
        <td>First port the ingest detector may use.</td></tr>
      <tr><td class="opt">ports.port_range_end</td><td class="opt">SQT_SERIES_PORTS__PORT_RANGE_END</td>
        <td class="def">12700</td>
        <td>Last port the ingest detector may use.</td></tr>
      <tr><td class="opt">ports.auto_detect</td><td class="opt">SQT_SERIES_PORTS__AUTO_DETECT</td>
        <td class="def">true</td>
        <td>Pick the first free ingest port in the range. False means bind
        <code>ingestion.port</code> exactly and fail if it is taken.</td></tr>
      <tr><td class="opt">ports.port_offset</td><td class="opt">SQT_SERIES_PORTS__PORT_OFFSET</td>
        <td class="def">0</td>
        <td>Shift the whole detection range, so a second instance can run
        beside the first: <code>12500</code> becomes
        <code>port_range_start + port_offset</code>.</td></tr>
    </tbody>
  </table>
"""

FRAGS["settings_tasks"] = r"""
  <table class="opts">
    <thead><tr><th>Key</th><th>Environment variable</th><th>Default</th><th>What it does</th></tr></thead>
    <tbody>
      <tr><td class="opt">rollup.enabled</td><td class="opt">SQT_SERIES_ROLLUP__ENABLED</td>
        <td class="def">true</td>
        <td>Keep the hourly summary table current. Off means wide queries scan
        raw rows, which is correct but slower.</td></tr>
      <tr><td class="opt">rollup.interval</td><td class="opt">SQT_SERIES_ROLLUP__INTERVAL</td>
        <td class="def">5m</td>
        <td>How often the rollup task runs. An interval string
        (<code>Ns/Nm/Nh/Nd</code>), not a count of hours — <code>5m</code> means
        "every five minutes", not "every five hours".</td></tr>
      <tr><td class="opt">retention.enabled</td><td class="opt">SQT_SERIES_RETENTION__ENABLED</td>
        <td class="def">true</td>
        <td>Drop expired partitions automatically.</td></tr>
      <tr><td class="opt">retention.default_ttl</td><td class="opt">SQT_SERIES_RETENTION__DEFAULT_TTL</td>
        <td class="def">30d</td>
        <td>How long data is kept. A TTL string: <code>s</code>, <code>m</code>,
        <code>h</code>, <code>d</code>, <code>w</code> — so <code>4w</code> is
        valid and <code>1m</code> means one month, not one minute.</td></tr>
      <tr><td class="opt">retention.check_interval</td><td class="opt">SQT_SERIES_RETENTION__CHECK_INTERVAL</td>
        <td class="def">1h</td>
        <td>How often retention runs. Also a TTL string.</td></tr>
      <tr><td class="opt">retention.partition_interval</td><td class="opt">SQT_SERIES_RETENTION__PARTITION_INTERVAL</td>
        <td class="def">month</td>
        <td>Partition width. <strong>Only <code>month</code> is supported.</strong>
        Retention drops whole partitions, so this is also the coarsest
        granularity at which anything is deleted.</td></tr>
      <tr><td class="opt">maintenance.enabled</td><td class="opt">SQT_SERIES_MAINTENANCE__ENABLED</td>
        <td class="def">true</td>
        <td>Run periodic <code>ANALYZE</code>.</td></tr>
      <tr><td class="opt">maintenance.analyze_interval</td><td class="opt">SQT_SERIES_MAINTENANCE__ANALYZE_INTERVAL</td>
        <td class="def">1h</td>
        <td>How often <code>ANALYZE</code> runs. An interval string.</td></tr>
      <tr><td class="opt">backup.enabled</td><td class="opt">SQT_SERIES_BACKUP__ENABLED</td>
        <td class="def">true</td>
        <td>Snapshot on a schedule.</td></tr>
      <tr><td class="opt">backup.interval</td><td class="opt">SQT_SERIES_BACKUP__INTERVAL</td>
        <td class="def">24h</td>
        <td>Seconds between snapshots. An interval string.</td></tr>
      <tr><td class="opt">backup.path</td><td class="opt">SQT_SERIES_BACKUP__PATH</td>
        <td class="def">~/.sqtseries/backups/</td>
        <td>Directory for snapshots. Created if absent. Each file is
        <code>sqtseries-YYYYmmdd-HHMMSS.db</code>; a name that already exists is
        kept, not overwritten.</td></tr>
      <tr><td class="opt">logging.level</td><td class="opt">SQT_SERIES_LOGGING__LEVEL</td>
        <td class="def">INFO</td>
        <td>One of <code>DEBUG</code>, <code>INFO</code>, <code>WARNING</code>,
        <code>ERROR</code>, <code>CRITICAL</code>.</td></tr>
      <tr><td class="opt">logging.format</td><td class="opt">SQT_SERIES_LOGGING__FORMAT</td>
        <td class="def">console</td>
        <td><code>console</code> for humans, <code>json</code> for a log
        collector.</td></tr>
      <tr><td class="opt">logging.file</td><td class="opt">SQT_SERIES_LOGGING__FILE</td>
        <td class="def">none</td>
        <td>Also write logs to this path. Unset means stderr only.</td></tr>
      <tr><td class="opt">config_file</td><td class="opt">SQT_SERIES_CONFIG_FILE</td>
        <td class="def">none</td>
        <td>Which settings file to load, when <code>--config</code> is not
        given. The only single-underscore variable: it is not a nested setting,
        so it has no <code>__</code>.</td></tr>
    </tbody>
  </table>
"""

# ------------------------------------------------------------------ routes ---

FRAGS["routes_table"] = r"""
  <table class="routes">
    <thead><tr><th>Method</th><th>Path</th><th>Purpose</th></tr></thead>
    <tbody>
      <tr><td class="opt">GET</td><td class="opt">/api/v1/health</td>
        <td>Liveness. <code>{"status":"ok","version":"0.1.0"}</code>, or
        <code>degraded</code> when the database is not ready. Never fails on a
        database problem, so a load balancer can poll it safely.</td></tr>
      <tr><td class="opt">POST</td><td class="opt">/api/v1/write</td>
        <td>Store one measurement or a batch. Republishes to live subscribers,
        exactly as the ZMQ path does.</td></tr>
      <tr><td class="opt">GET</td><td class="opt">/api/v1/read</td>
        <td>Read a metric: raw rows, or bucketed with
        <code>aggregation</code> + <code>interval</code>.</td></tr>
      <tr><td class="opt">GET</td><td class="opt">/api/v1/aggregate</td>
        <td>Several aggregates over one window in a single pass.</td></tr>
      <tr><td class="opt">GET</td><td class="opt">/api/v1/stats</td>
        <td>Metric and series counts.</td></tr>
      <tr><td class="opt">GET</td><td class="opt">/api/v1/connections</td>
        <td>Every live WebSocket connection with its peer, topic and arrival
        time.</td></tr>
      <tr><td class="opt">GET</td><td class="opt">/api/v1/subscribers</td>
        <td>Every topic ever seen, with its live count, plus every stored
        metric at zero.</td></tr>
      <tr><td class="opt">GET</td><td class="opt">/ws/subscribe</td>
        <td>WebSocket. Live measurements for one topic.</td></tr>
      <tr><td class="opt">GET</td><td class="opt">/ws/connections</td>
        <td>WebSocket. One snapshot, then a frame per connect/disconnect and
        subscription change.</td></tr>
      <tr><td class="opt">GET</td><td class="opt">/ws/dashboard</td>
        <td>WebSocket. The admin dashboard feed: snapshot, live events, and a
        counter tick every second.</td></tr>
      <tr><td class="opt">GET</td><td class="opt">/</td>
        <td>The admin dashboard page.</td></tr>
      <tr><td class="opt">GET</td><td class="opt">/dashboard</td>
        <td>The same page, on a path that reads well in a bookmark.</td></tr>
      <tr><td class="opt">GET</td><td class="opt">/dashboard-assets/*</td>
        <td>Dashboard JavaScript, CSS, icons, and the vendored Swagger and
        ReDoc bundles. No CDN: the gateway's own CSP forbids one.</td></tr>
      <tr><td class="opt">GET</td><td class="opt">/api-docs</td>
        <td>Self-hosted Swagger UI, reading <code>/openapi.json</code>.</td></tr>
      <tr><td class="opt">GET</td><td class="opt">/redoc</td>
        <td>Self-hosted ReDoc, same schema.</td></tr>
      <tr><td class="opt">GET</td><td class="opt">/openapi.json</td>
        <td>The machine-readable schema. The single source for a generated
        client.</td></tr>
      <tr><td class="opt">GET</td><td class="opt">/docs</td>
        <td>These pages, served by the service itself.</td></tr>
    </tbody>
  </table>
"""

FRAGS["error_codes"] = r"""
  <table>
    <thead><tr><th>Code</th><th>Where</th><th>Meaning</th></tr></thead>
    <tbody>
      <tr><td class="opt">INVALID_REQUEST</td><td>ZMQ query/admin</td>
        <td>The frame was not a valid message object, or an admin
        <code>cmd</code> was not a string.</td></tr>
      <tr><td class="opt">INVALID_QUERY</td><td>ZMQ query</td>
        <td>The message parsed but the query is wrong: an unknown aggregation,
        a bad <code>limit</code>, a missing metric. Distinct from
        <code>INTERNAL_ERROR</code> so a client bug is never reported as a
        server fault.</td></tr>
      <tr><td class="opt">QUERY_TIMEOUT</td><td>ZMQ query</td>
        <td>The handler exceeded <code>query.timeout_s</code>.</td></tr>
      <tr><td class="opt">MAX_ROWS_EXCEEDED</td><td>ZMQ query</td>
        <td>The result would exceed <code>query.max_rows</code>. Narrow the
        window, add an aggregation, or raise the cap.</td></tr>
      <tr><td class="opt">INTERNAL_ERROR</td><td>ZMQ query</td>
        <td>An unexpected failure. The detail is in the log, not the reply.</td></tr>
      <tr><td class="opt">NOT_READY</td><td>ZMQ query/admin</td>
        <td>The engine is not available yet, or not at all.</td></tr>
      <tr><td class="opt">BACKUP_FAILED</td><td>ZMQ admin</td>
        <td><code>VACUUM INTO</code> failed.</td></tr>
      <tr><td class="opt">VACUUM_BUSY</td><td>ZMQ admin</td>
        <td>A <code>VACUUM</code> was refused because the database is in use.
        The message says to stop the service and use
        <code>sqtseries vacuum</code>.</td></tr>
      <tr><td class="opt">RATE_LIMITED</td><td>HTTP <code>429</code></td>
        <td>Over <code>http.rate_limit_per_minute</code> for this peer in the
        current one-minute window.</td></tr>
      <tr><td class="opt">400</td><td>HTTP</td>
        <td>Bad input: missing or empty <code>metric</code>, a
        non-numeric or non-finite <code>value</code>, tags that are not
        string-to-string, an unparseable timestamp, a timestamp outside
        signed 64-bit nanoseconds, or a client timestamp beyond the clock-skew
        window.</td></tr>
      <tr><td class="opt">413</td><td>HTTP</td>
        <td>The result exceeds <code>query.max_rows</code>. The request was
        valid; the answer is too big to produce under the bounded-work policy.</td></tr>
      <tr><td class="opt">504</td><td>HTTP</td>
        <td>The query exceeded <code>query.timeout_s</code>.</td></tr>
    </tbody>
  </table>
"""

# ------------------------------------------------------------ aggregations ---

FRAGS["agg_table"] = r"""
  <table>
    <thead><tr><th>Name</th><th>Result over a window</th><th>Empty window</th><th>From the hourly rollup?</th></tr></thead>
    <tbody>
      <tr><td class="opt">avg</td><td>Arithmetic mean.</td><td class="muted">NaN</td><td>yes</td></tr>
      <tr><td class="opt">sum</td><td>Total.</td><td><code>0.0</code></td><td>yes</td></tr>
      <tr><td class="opt">min</td><td>Smallest value.</td><td class="muted">NaN</td><td>yes</td></tr>
      <tr><td class="opt">max</td><td>Largest value.</td><td class="muted">NaN</td><td>yes</td></tr>
      <tr><td class="opt">count</td><td>Number of points.</td><td><code>0.0</code></td><td>yes</td></tr>
      <tr><td class="opt">first</td><td>Oldest value in the bucket.</td><td class="muted">NaN</td><td class="muted">no</td></tr>
      <tr><td class="opt">last</td><td>Newest value in the bucket.</td><td class="muted">NaN</td><td class="muted">no</td></tr>
      <tr><td class="opt">median</td><td>Middle value.</td><td class="muted">NaN</td><td class="muted">no</td></tr>
      <tr><td class="opt">p95</td><td>95th percentile, linearly interpolated between the two nearest ranks.</td><td class="muted">NaN</td><td class="muted">no</td></tr>
      <tr><td class="opt">p99</td><td>99th percentile, same method.</td><td class="muted">NaN</td><td class="muted">no</td></tr>
    </tbody>
  </table>
  <p class="small muted">The last column is the important one. The hourly
  summary stores only count, sum, min and max, so those five are answered from
  it and the other five must read the raw rows. That is a speed difference, not
  a correctness one: every answer is exact either way. <code>min</code>,
  <code>max</code>, <code>avg</code>, <code>sum</code> and <code>count</code>
  are also the only functions SQLite can bucket inside the database, so
  <code>aggregation</code> + <code>interval</code> in that set never
  materialises a raw row in Python.</p>
"""

# ------------------------------------------------------------ wire format ---

FRAGS["wire_ingest"] = r"""
  <table>
    <thead><tr><th>Field</th><th>Type</th><th>Required</th><th>Default</th><th>Rule</th></tr></thead>
    <tbody>
      <tr><td class="opt">metric</td><td>string</td><td>yes</td><td class="muted">—</td>
        <td>Non-empty. The name of the thing being measured.</td></tr>
      <tr><td class="opt">value</td><td>number</td><td>yes</td><td class="muted">—</td>
        <td>Must be finite. A boolean is rejected even though Python treats it
        as an integer.</td></tr>
      <tr><td class="opt">tags</td><td>object</td><td class="muted">no</td>
        <td class="def">null</td>
        <td>String keys to string values, all of them. The pairs form the
        series identity: the same metric with different tags is a different
        series.</td></tr>
      <tr><td class="opt">timestamp</td><td>number or ISO-8601 string</td>
        <td class="muted">no</td><td class="def">server clock</td>
        <td>Epoch seconds, or an ISO-8601 string with up to nanosecond
        precision. Outside the clock-skew window is refused. Server-assigned
        timestamps are forced strictly increasing, so a burst can never
        collide on the primary key.</td></tr>
    </tbody>
  </table>
"""

FRAGS["wire_query"] = r"""
  <table>
    <thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Rule</th></tr></thead>
    <tbody>
      <tr><td class="opt">metric</td><td>string</td><td class="muted">required</td>
        <td>Non-empty.</td></tr>
      <tr><td class="opt">start</td><td>integer ns or ISO-8601</td><td class="def">null</td>
        <td>Over ZMQ this is <strong>epoch nanoseconds</strong>, not seconds.
        The HTTP route takes seconds. A string is converted before validation.</td></tr>
      <tr><td class="opt">end</td><td>integer ns or ISO-8601</td><td class="def">null</td>
        <td>Inclusive bound.</td></tr>
      <tr><td class="opt">aggregation</td><td>string</td><td class="def">null</td>
        <td>One of the ten names. Unset returns raw rows.</td></tr>
      <tr><td class="opt">interval</td><td>string</td><td class="def">null</td>
        <td>Bucket width, <code>Ns/Nm/Nh/Nd</code>. Needs
        <code>aggregation</code>: alone it does nothing.</td></tr>
      <tr><td class="opt">aggregations</td><td>comma-separated string</td>
        <td class="def">null</td>
        <td>Several functions in one pass. Wins over
        <code>aggregation</code>.</td></tr>
      <tr><td class="opt">limit</td><td>integer</td><td class="def">null</td>
        <td>Must be ≥ 1. On a raw read it bounds the SQL fetch; with an
        aggregation it bounds the number of result rows, because the transform
        still needs every row in the window.</td></tr>
      <tr><td class="opt">order</td><td><code>asc</code> or <code>desc</code></td>
        <td class="def">asc</td>
        <td>Result order.</td></tr>
      <tr><td class="opt">fill_gaps_ns</td><td>integer ns</td><td class="def">null</td>
        <td>Embedded Python API only. Inserts an interpolated midpoint across
        gaps no larger than this.</td></tr>
    </tbody>
  </table>
"""

# ------------------------------------------------------------- admin cmds ---

FRAGS["admin_table"] = r"""
  <table>
    <thead><tr><th>Command</th><th>Request</th><th>Reply</th></tr></thead>
    <tbody>
      <tr><td class="opt">ping</td><td><code>{"cmd":"ping"}</code></td>
        <td><code>{"status":"ok","pong":true}</code></td></tr>
      <tr><td class="opt">health</td><td><code>{"cmd":"health"}</code></td>
        <td>Uptime, database reachability, WAL size.</td></tr>
      <tr><td class="opt">stats</td><td><code>{"cmd":"stats"}</code></td>
        <td>Every service counter. See the field table below.</td></tr>
      <tr><td class="opt">connections</td><td><code>{"cmd":"connections"}</code></td>
        <td>Every live WebSocket connection.</td></tr>
      <tr><td class="opt">conncheck</td><td><code>{"cmd":"conncheck","ids":["a","b"]}</code></td>
        <td><code>{"status":"ok","present":["a"]}</code> — which of the given ids
        are still connected. An empty <code>ids</code> is valid and returns an
        empty list.</td></tr>
      <tr><td class="opt">subscribers</td><td><code>{"cmd":"subscribers"}</code></td>
        <td>ZMQ subscriber total plus the merged topic list.</td></tr>
      <tr><td class="opt">optimize</td><td><code>{"cmd":"optimize"}</code></td>
        <td><code>{"status":"ok","optimized":true}</code></td></tr>
      <tr><td class="opt">backup</td><td><code>{"cmd":"backup"}</code></td>
        <td>The path written, or <code>BACKUP_FAILED</code>.</td></tr>
      <tr><td class="opt">vacuum</td><td><code>{"cmd":"vacuum"}</code></td>
        <td><code>{"status":"ok","vacuumed":true}</code>, or
        <code>VACUUM_BUSY</code> telling you to stop the service. The admin
        socket has no handler timeout, because a large backup legitimately runs
        long — but it holds the single SQLite writer, so ingest pauses for the
        duration. Use the offline CLI for anything big.</td></tr>
    </tbody>
  </table>
  <p class="small muted">Any other <code>cmd</code> is refused with
  <code>INVALID_REQUEST</code>. Admin commands are not rate-limited and not
  authenticated: the socket binds loopback only, and that binding is the
  access control.</p>
"""

FRAGS["admin_stats_fields"] = r"""
  <table>
    <thead><tr><th>Field</th><th>Means</th></tr></thead>
    <tbody>
      <tr><td class="opt">uptime_s</td><td>Seconds since the service started.</td></tr>
      <tr><td class="opt">ingested</td><td>Points accepted for storage, from
      both transports. <code>recv</code> over ZMQ plus HTTP writes.</td></tr>
      <tr><td class="opt">persisted</td><td>Points actually committed. The
      difference from <code>ingested</code> is work still in flight.</td></tr>
      <tr><td class="opt">dropped</td><td>Points in a batch whose insert
      failed after every retry. Counted, never silent — the batch is dropped
      whole and visible here. A transient SQLite error (a locked database, a
      momentary I/O fault) is retried first, so a non-zero
      <code>sink_retries</code> with <code>dropped: 0</code> means the retry
      saved data that would otherwise have been lost.</td></tr>
      <tr><td class="opt">sink_retries</td><td>Batches re-run after a
      transient <code>sqlite3.OperationalError</code>. The ZeroMQ ingest socket
      has no acknowledgements, so once a frame is drained the producer cannot
      re-send it — retrying is the only way a momentary failure does not destroy
      accepted data. A climbing count is an early warning for a failing
      disk.</td></tr>
      <tr><td class="opt">invalid</td><td>Frames refused as malformed or as
      breaching the clock-skew window.</td></tr>
      <tr><td class="opt">ingest_errors</td><td>Socket-level receive errors.</td></tr>
      <tr><td class="opt">queries</td><td>Query requests served, both
      transports.</td></tr>
      <tr><td class="opt">query_errors</td><td>Handler exceptions.</td></tr>
      <tr><td class="opt">queries_shed</td><td>Requests left in the socket pipe
      because <code>max_inflight</code> was reached. Backpressure made
      visible.</td></tr>
      <tr><td class="opt">replies_dropped</td><td>Replies that found no peer.
      The requester had already gone; counted rather than lost quietly.</td></tr>
      <tr><td class="opt">admin_requests</td><td>Admin commands served.</td></tr>
      <tr><td class="opt">published</td><td>Publish <em>attempts</em>, not
      deliveries. A slow subscriber's frame is still counted here, so this is
      never a per-subscriber receipt.</td></tr>
      <tr><td class="opt">subscribers</td><td>Active stream topics.</td></tr>
      <tr><td class="opt">series</td><td>Distinct (metric, tags) pairs.</td></tr>
      <tr><td class="opt">metrics</td><td>Distinct metric names.</td></tr>
      <tr><td class="opt">wal_bytes</td><td>Current WAL size.</td></tr>
      <tr><td class="opt">checkpoints</td><td>Checkpoints completed.</td></tr>
      <tr><td class="opt">checkpoint_busy_runs</td><td>Checkpoint attempts
      blocked by a long reader. A number that keeps climbing means something is
      holding a read snapshot open.</td></tr>
      <tr><td class="opt">analyze_runs</td><td><code>ANALYZE</code> passes
      completed.</td></tr>
      <tr><td class="opt">retention_runs</td><td>Retention passes
      completed.</td></tr>
      <tr><td class="opt">partitions_dropped</td><td>Partitions dropped by
      retention, since start.</td></tr>
      <tr><td class="opt">backup_runs</td><td>Backup passes, including ones
      that found a fresh snapshot already present.</td></tr>
      <tr><td class="opt">backups_created</td><td>Snapshots actually
      written.</td></tr>
      <tr><td class="opt">last_backup</td><td>Path of the most recent
      snapshot.</td></tr>
      <tr><td class="opt">query_cache_size</td><td>Entries held.</td></tr>
      <tr><td class="opt">query_cache_hits</td><td>Queries served from
      cache.</td></tr>
      <tr><td class="opt">query_cache_misses</td><td>Queries that had to run.
      A low hit rate with a busy dashboard usually means the polling interval is
      longer than the cache TTL.</td></tr>
    </tbody>
  </table>
"""


# ------------------------------------------------------------- computed ------


def _src_loc() -> str:
    """Lines of Python under ``src/``, counted at build time.

    Every other table here is transcribed by hand and ``verify_docs.py`` proves
    the transcription still matches the code. A line count cannot be checked that
    way, and it went stale the moment any source file changed -- so it is counted
    here instead. One owner, no hand-maintained number to rot.
    """
    root = pathlib.Path(__file__).resolve().parents[2] / "src"
    total = sum(len(p.read_text().splitlines()) for p in sorted(root.rglob("*.py")))
    return f"{total:,}"


FRAGS["src_loc"] = _src_loc()

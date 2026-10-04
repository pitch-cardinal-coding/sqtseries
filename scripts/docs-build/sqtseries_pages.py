"""Per-page shell text for the sqtseries docs.

Each entry supplies only the shell: the browser title, the meta description, the
eyebrow, the heading, the tagline, the meta strip, and the six footer items. The
body — prose, tables, commands, diagrams — lives in ``bodies/<stem>.html``.

Keeping the shell here rather than in the generator is deliberate: the
generator holds the stylesheet, which is the part that must not drift, and a
style change should stay a one-file diff.

Prose is written as triple-quoted blocks so a line break in the source is a
space in the output and never a broken string literal.
"""

from sqtseries_build import register

PAGES = []


def page(stem, title, description, eyebrow, h1, tagline, meta, foot):
    PAGES.append(stem)
    register(
        stem,
        title=title,
        description=description,
        eyebrow=eyebrow,
        h1=h1,
        tagline=tagline,
        meta=meta,
        foot=foot,
    )


def registered():
    return list(PAGES)


page(
    "index",
    title="sqtseries — the runbook index: every page in one place",
    description=(
        "Every sqtseries page in one place: install, configure, ingest, query, "
        "stream, back up, and serve. Each page is self-contained."
    ),
    eyebrow="Index",
    h1="Every page",
    tagline=(
        "Fifteen pages, grouped by what you are trying to do. Each one stands on "
        "its own: every setting, every option, every command, and the numbers "
        "behind the claims."
    ),
    meta=[
        ("Pages", "15, all self-contained"),
        ("Start with", "Quick Start for the whole cycle"),
        ("Storage", "one SQLite file, WAL, monthly partitions"),
        ("Transports", "ZMQ and HTTP, one wire format"),
        ("Python", "3.14+, <code>pip install sqtseries</code>"),
    ],
    foot=[
        (
            "New to sqtseries?",
            """
            Read <a href="quickstart.html">Quick Start</a> first. It walks the
            whole cycle — install, run, write, query, subscribe, and put the
            service under systemd — and every other page assumes it.
            """,
        ),
        (
            "What do you want to do?",
            """
            Sending numbers in: <a href="ingestion.html">Ingestion</a>. Asking
            questions of the past: <a href="queries.html">Queries</a>. Watching
            values arrive: <a href="streaming.html">Streaming</a> and
            <a href="dashboard.html">Dashboard</a>.
            """,
        ),
        (
            "Talking to it from code",
            """
            The wire format is identical on every transport, so there is no client
            library to install. Copy-paste samples for Python, Go, Rust, PHP,
            Node.js and the shell are on
            <a href="clients.html">Client Libraries</a>; the exact shapes are on
            <a href="api.html">API Reference</a>.
            """,
        ),
        (
            "Before you change anything",
            """
            <a href="configuration.html">Configuration</a> lists every setting,
            every environment variable, and every validation rule that can stop
            the service from starting. Read it before editing a port.
            """,
        ),
        (
            "When it is not working",
            """
            <a href="api.html#admin">Admin commands</a> answer over the admin
            socket without a client library: <code>health</code>,
            <code>stats</code>, <code>connections</code>,
            <code>conncheck</code>, <code>subscribers</code>.
            """,
        ),
        (
            "These pages are generated",
            """
            Do not edit the HTML in <code>dist/docs/</code>. Edit the body source
            and rebuild — see <code>scripts/docs-build/README.md</code>.
            """,
        ),
    ],
)

page(
    "quickstart",
    title="sqtseries — Quick Start: the whole cycle in one page",
    description=(
        "Install sqtseries, start the service, write a measurement, query it "
        "back, subscribe live, and run it as a systemd unit."
    ),
    eyebrow="Runbook · the whole cycle",
    h1="Quick Start",
    tagline=(
        "From nothing to a running service, a stored measurement, an answer, a "
        "live subscription, and a unit file that starts on boot. The shortest "
        "honest path through the product."
    ),
    meta=[
        ("Install", "<code>pip install sqtseries</code>"),
        ("Run", "<code>sqtseries run</code>, prints the links"),
        ("Store", "one file at <code>~/.sqtseries/data/db.sqlite</code>"),
        ("Ports", "six, five fixed and one auto-detected"),
        ("Tested", "every command below was run as written"),
    ],
    foot=[
        (
            "Order matters",
            """
            The service must be up before anything else on this page. Every
            example after step 1 talks to a running service on its ports.
            """,
        ),
        (
            "Two ways to send data",
            """
            Over HTTP with <code>curl</code>, or over ZeroMQ with the bundled
            Python <code>Client</code>. Both store the same shape. HTTP is the
            one to reach for from a language with no ZMQ binding.
            """,
        ),
        (
            "Timestamps: seconds over HTTP, nanoseconds over ZMQ",
            """
            This trips up nearly everyone. The HTTP routes take and return
            <strong>epoch seconds</strong>; the ZMQ query message takes
            <strong>epoch nanoseconds</strong> and returns seconds. Both are
            covered on <a href="ingestion.html">Ingestion</a> and
            <a href="api.html">API Reference</a>.
            """,
        ),
        (
            "Where to go next",
            """
            Settings: <a href="configuration.html">Configuration</a>. More
            questions: <a href="queries.html">Queries</a>. Running it properly:
            <a href="systemd.html">Systemd</a>.
            """,
        ),
        (
            "If a command fails",
            """
            <code>sqtseries health</code> checks the database file directly and
            needs no service. <code>sqtseries status</code> shows the real ports
            and pid, or says the runtime file is stale.
            """,
        ),
        (
            "One instance per database",
            """
            <code>run</code> refuses to start over a database another live
            instance is serving. To run a second service, give it its own
            <code>--db</code> and its own ports — see
            <a href="configuration.html">Configuration</a>.
            """,
        ),
    ],
)

page(
    "configuration",
    title="sqtseries — Configuration: every setting, every variable",
    description=(
        "Every sqtseries setting: the TOML key, the environment variable, the "
        "default, the validation rule, and the port allocation mechanism."
    ),
    eyebrow="Reference · settings",
    h1="Configuration",
    tagline=(
        "Every setting the service reads, with the exact key, the environment "
        "variable that overrides it, the default in the code, and the rule that "
        "refuses a bad value at start-up rather than halfway through a run."
    ),
    meta=[
        ("Sections", "13 in the settings tree"),
        ("Override order", "defaults, file, environment, CLI"),
        ("Formats", "TOML, YAML, JSON by suffix"),
        ("Prefix", "<code>SQT_SERIES_</code>, nested with <code>__</code>"),
        ("Durations", "two different syntaxes — read the note"),
    ],
    foot=[
        (
            "Two different duration syntaxes",
            """
            This is the single most common configuration mistake. <strong>Interval
            strings</strong> (<code>rollup.interval</code>,
            <code>backup.interval</code>,
            <code>maintenance.analyze_interval</code>) take
            <code>Ns/Nm/Nh/Nd</code> and nothing else. <strong>TTL strings</strong>
            (<code>retention.default_ttl</code>,
            <code>retention.check_interval</code>) take
            <code>Ns/Nm/Nh/Nd/Nw</code> and <em>also</em> accept <code>w</code>.
            In both, <code>m</code> means <strong>minutes</strong> — there is no
            month syntax anywhere. Retention additionally drops whole monthly
            partitions, so a TTL is applied at partition granularity.
            """,
        ),
        (
            "Override order, lowest first",
            """
            1. The default in the code. 2. The config file, from
            <code>--config</code> or <code>SQT_SERIES_CONFIG_FILE</code>.
            3. Environment variables. 4. <code>--db</code>, which is applied last
            of all. The loader only lets the file supply a leaf the environment
            did not set, so a variable always beats a file.
            """,
        ),
        (
            "Ports are resolved twice",
            """
            The five fixed ports are read straight from settings. The ingest port
            may be reallocated by the detector. Always read the real values from
            <code>sqtseries status</code> rather than assuming.
            """,
        ),
        (
            "Validation refuses to start",
            """
            Every rule in <code>validate_settings</code> is checked before the
            database is opened. A bad value is an error message naming the key,
            not a service that starts and then misbehaves.
            """,
        ),
        (
            "Security settings to review before exposing it",
            """
            <code>http.host</code> defaults to <code>0.0.0.0</code> and
            <code>http.cors_origins</code> to <code>["*"]</code>, with no
            authentication on the HTTP side. On an untrusted network set both to
            loopback and put a proxy in front. The ZMQ ports are already
            loopback-only.
            """,
        ),
        (
            "A full example, every section",
            """
            The <a href="#complete-example">complete example</a> further down this
            page sets every key once, with a comment on each, so you can copy a
            known-good starting point.
            """,
        ),
    ],
)

page(
    "ingestion",
    title="sqtseries — Ingestion: getting numbers in",
    description=(
        "Write measurements into sqtseries over ZeroMQ PUSH, over HTTP, or from "
        "the bundled Python Client. Wire format, tags, timestamps, backpressure."
    ),
    eyebrow="Runbook · writing data",
    h1="Ingestion",
    tagline=(
        "Three ways in — a ZeroMQ PUSH socket, an HTTP POST, and the bundled "
        "Python Client — carrying the identical JSON shape. Plus what a tag "
        "really is, how timestamps behave, and what happens when a producer "
        "outruns the disk."
    ),
    meta=[
        ("Transports", "ZMQ PULL, HTTP POST, Python Client"),
        ("Shape", "one JSON object per measurement"),
        ("Tags", "string to string, part of series identity"),
        ("Timestamps", "epoch or ISO-8601, ns precision"),
        ("Backpressure", "bounded, loud, never silent"),
    ],
    foot=[
        (
            "One point, one frame",
            """
            There is no batch envelope on the ZMQ path: a PUSH frame is one
            measurement. Throughput comes from many frames and from the
            per-transaction commit, not from a bigger payload. The HTTP route does
            accept a JSON array, and validates the whole array before storing any
            of it, so a bad item never leaves a half-written batch.
            """,
        ),
        (
            "Tags are dimensions, not identifiers",
            """
            A series is the pair (metric, tags). The same metric with different
            tags is a different series with its own rollup rows and its own query
            results. Values are strings: the storage layer JSON-encodes them into
            one column and a <code>CHECK</code> constraint rejects anything that
            is not valid JSON, so a malformed tag set cannot be written at all.
            """,
        ),
        (
            "Timestamps, and the trap",
            """
            Send epoch seconds, or an ISO-8601 string with up to nine fractional
            digits. A naive string is read as UTC. A string whose absolute value
            exceeds <code>1e12</code> is treated as nanoseconds, which is how an
            ISO-8601 value survives the JSON round trip without losing its
            precision. Beyond the clock-skew window the write is refused.
            """,
        ),
        (
            "Backpressure is a feature",
            """
            When the persister falls behind, the receive loop stops reading, frames
            buffer to the socket HWM, and then senders block. Nothing is dropped to
            keep up. A batch whose insert fails is dropped whole and counted in
            <code>dropped</code> on the admin socket, so a loss is always visible
            rather than inferred.
            """,
        ),
        (
            "Choosing a transport",
            """
            ZeroMQ for sustained high rates from long-lived processes, because it
            is fire-and-forget and never blocks the writer on a reply. HTTP for
            shell scripts, browsers, and any language without a ZMQ binding. Use
            both if you like — they converge on the same table and the same live
            stream.
            """,
        ),
        (
            "The embedded engine needs no service at all",
            """
            For a single-process program you can skip every socket and open the
            engine in-process — see the embedded API on
            <a href="api.html#embedded">API Reference</a>.
            """,
        ),
    ],
)

page(
    "queries",
    title="sqtseries — Queries: asking questions of the past",
    description=(
        "Query sqtseries: time ranges, all ten aggregations, interval bucketing, "
        "downsampling, gap filling, limits, and the hourly rollup fast path."
    ),
    eyebrow="Runbook · reading data",
    h1="Queries",
    tagline=(
        "Read a metric over a window, raw or aggregated, bucketed or "
        "whole-window, with the exact rules for limits, ordering, empty windows, "
        "and the point at which a query is refused for being too large."
    ),
    meta=[
        ("Aggregations", "10, exact — never approximate"),
        ("Intervals", "<code>Ns/Nm/Nh/Nd</code>, epoch-aligned"),
        ("Fast path", "hourly rollup for 5 of the 10"),
        ("Row cap", "<code>query.max_rows</code>, 413 not truncation"),
        ("Units", "ns in over ZMQ, seconds in over HTTP"),
    ],
    foot=[
        (
            "Raw or aggregated, and how they differ",
            """
            With no <code>aggregation</code> you get the stored points. With
            <code>aggregation</code> alone you get one value for the whole window,
            anchored at the oldest sample in it. With <code>aggregation</code> and
            <code>interval</code> you get one value per bucket, each anchored at
            its bucket start.
            """,
        ),
        (
            "Buckets are epoch-aligned",
            """
            A bucket is <code>timestamp // interval</code>, not a window that
            starts at your first sample. With <code>interval=1h</code> the bucket
            is the top of the hour, whatever time you asked from. That is what
            makes two queries over overlapping windows agree.
            """,
        ),
        (
            "Why some aggregates are slower",
            """
            The hourly summary stores count, sum, min and max. Those five
            aggregations, and only those, can be answered from it.
            <code>median</code>, <code>p95</code>, <code>p99</code>,
            <code>first</code> and <code>last</code> must read the raw rows. The
            answer is identical either way — this is a speed difference only, and
            it is worth knowing before you build a percentile dashboard.
            """,
        ),
        (
            "The rollup only covers finished hours",
            """
            An hour is summarised once the clock is past it, plus the clock-skew
            window, so a write can never land in an hour already summarised. The
            current hour is therefore always read live. A query that spans fewer
            than two hours skips the fast path entirely: there is nothing to
            accelerate and the raw path is faster.
            """,
        ),
        (
            "Too big is an error, not a short answer",
            """
            A read that would materialise more than <code>query.max_rows</code>
            raw rows raises <code>MAX_ROWS_EXCEEDED</code> over ZMQ and
            <code>413</code> over HTTP. The check is a <code>COUNT(*)</code>
            before the fetch, so you are not charged for rows you will not get.
            The fix is always one of: narrow the window, add an aggregation, or
            raise the cap.
            """,
        ),
        (
            "Results are cached for five seconds",
            """
            Identical queries inside the TTL are served from a bounded cache,
            which is what makes a dashboard that polls the same metric every second
            cheap. Watch <code>query_cache_hits</code> and
            <code>query_cache_misses</code> on the admin socket to see it working.
            """,
        ),
    ],
)

page(
    "streaming",
    title="sqtseries — Streaming: watching values arrive",
    description=(
        "Subscribe to live sqtseries measurements over ZeroMQ SUB, WebSocket, or "
        "the Python Client. Topic matching, bounded fan-out, keepalives, stats."
    ),
    eyebrow="Runbook · live data",
    h1="Streaming",
    tagline=(
        "Three live paths — a ZeroMQ SUB socket, the <code>/ws/subscribe</code> "
        "WebSocket, and the bundled Client — plus a fourth that reports who is "
        "connected. What a slow subscriber costs, and how a topic filter is "
        "matched."
    ),
    meta=[
        ("Stream", "XPUB on 12503, two-part frames"),
        ("WebSocket", "<code>/ws/subscribe</code>, push only"),
        ("Monitoring", "<code>/ws/connections</code>, event driven"),
        ("Filter", "boundary aware, no prefix bleed"),
        ("Slow client", "its own queue, counted drops"),
    ],
    foot=[
        (
            "A published value is not a stored value yet",
            """
            Publishing happens strictly after the commit. A subscriber can
            therefore never see a point that later failed to persist, which is the
            property that makes the live stream trustworthy for alerting.
            """,
        ),
        (
            "Topic matching is boundary aware",
            """
            <code>*</code> takes everything. A pattern ending in a dot is a plain
            prefix, so <code>cpu.</code> matches <code>cpu.load</code>. A
            dot-less pattern matches the exact topic and its dot-delimited
            children, so <code>cpu</code> matches <code>cpu.load</code> but
            <strong>not</strong> <code>cpu10</code>. Plain prefix matching used
            to fan one topic out to ten under stress traffic; that is why the
            boundary is checked.
            """,
        ),
        (
            "A slow subscriber costs only itself",
            """
            Each WebSocket subscriber owns a queue bounded by
            <strong>bytes</strong>, not by message count. When it overflows, the
            <em>newest</em> frame is dropped and counted — the queue already holds
            that client's recent history, so a slow consumer loses the live tail
            rather than the backlog. The publisher never blocks and never notices.
            """,
        ),
        (
            "XPUB means the counts are exact",
            """
            The stream socket is an XPUB with verboser mode on, so the service
            receives a subscribe event for <em>every</em> join and leave on the
            wire rather than only when a topic is first heard of. Two clients on
            one topic report as two. There is no polling and no estimate.
            """,
        ),
        (
            "Keepalives at two levels",
            """
            The WebSocket send loop emits <code>{"type":"ping"}</code> after 30 s
            of silence, uvicorn sends a protocol-level ping every 20 s, and a
            frame that cannot be flushed within 30 s evicts the client as a slow
            consumer. Inbound frames on a subscribe socket are consumed and
            ignored, so a disconnect surfaces at once instead of at the next send.
            """,
        ),
        (
            "The stats socket is the audit trail",
            """
            Port 12506 publishes <code>conn</code>, <code>sub</code> and a
            periodic <code>report</code> event. Subscribe with an empty prefix to
            take everything, or with <code>conn</code> or <code>sub</code> to
            take one kind. It is how you answer “who is watching what” without
            polling the service.
            """,
        ),
    ],
)

page(
    "dashboard",
    title="sqtseries — Admin Dashboard: the service on screen",
    description=(
        "The sqtseries live admin dashboard: health, ingest rates, connections, "
        "topics, storage, and the WebSocket protocol behind it."
    ),
    eyebrow="Runbook · operations",
    h1="Admin Dashboard",
    tagline=(
        "A live page served by the service itself, pushed second by second over "
        "a WebSocket. Health, ingest and query rates, every connection, every "
        "topic, storage size, and the rollup watermark."
    ),
    meta=[
        ("URL", "<code>/dashboard</code> on the HTTP port"),
        ("Feed", "<code>/ws/dashboard</code>, no polling"),
        ("Tick", "counters every 1 s, events as they happen"),
        ("Assets", "vendored locally, no CDN"),
        ("Mobile", "responsive, tested at phone width"),
    ],
    foot=[
        (
            "The page is not the source of truth",
            """
            Everything the page shows is available over the admin socket and the
            HTTP API. The page is a convenient view; a monitoring system should
            poll <code>health</code> and read <code>stats</code> instead of
            scraping a page.
            """,
        ),
        (
            "Why there is no polling",
            """
            The feed sends one snapshot on connect, then a frame per registry
            change, then a counter tick every second. Connections appear the
            moment they are made rather than up to a second late, and an idle
            service still produces a steady, cheap heartbeat.
            """,
        ),
        (
            "The topics panel remembers",
            """
            It lists every topic ever seen, with its live count — zero when idle —
            alongside the per-topic publish total. A topic that has gone quiet does
            not vanish from the list, which is what you want when you are trying to
            work out whether a producer stopped.
            """,
        ),
        (
            "Reading the numbers honestly",
            """
            <code>ingested</code> counts both transports.
            <code>persisted</code> trails it while work is in flight.
            <code>dropped</code> and <code>invalid</code> are the two you should
            alert on: a rising <code>dropped</code> means a batch insert is
            failing, and a rising <code>invalid</code> usually means a producer's
            clock has drifted.
            """,
        ),
        (
            "Self-hosted, deliberately",
            """
            The gateway serves its own Swagger UI and ReDoc from vendored assets
            and its own documentation from <code>/docs</code>, because the
            gateway's Content-Security-Policy forbids a CDN. Nothing on these pages
            reaches the internet.
            """,
        ),
        (
            "Seeing it for yourself",
            """
            The <a href="streaming.html">Streaming</a> page documents the feed frame
            by frame, and <a href="camera.html">Camera analytics</a> builds a second
            live page on top of the same three endpoints.
            """,
        ),
    ],
)

page(
    "camera",
    title="sqtseries — Camera analytics: a feed stored and watched live",
    description=(
        "Pump a live camera metrics feed into sqtseries, watch it on a live page, "
        "and answer questions about it afterwards."
    ),
    eyebrow="Runbook · worked example",
    h1="Camera analytics",
    tagline=(
        "A complete worked example: a script that generates realistic camera "
        "metrics, pushes them in over HTTP, and answers seventeen questions about "
        "the result — with a live page watching the same data as it lands."
    ),
    meta=[
        ("Scripts", "three, in <code>examples/camera/</code>"),
        ("Input", "a metrics URL, polled on an interval"),
        ("Storage", "the normal write path, nothing special"),
        ("Live page", "built on <code>/ws/subscribe</code>"),
        ("Tested", "run by the suite against a live service"),
    ],
    foot=[
        (
            "It is an ordinary write path",
            """
            The camera feed produces ordinary measurements with ordinary tags.
            Nothing in the engine knows or cares that the source is a camera, which
            is the point: the example is a workload, not a feature.
            """,
        ),
        (
            "Why the live page exists",
            """
            The stock dashboard shows <em>service</em> health. This second page
            shows <em>data</em> — the metric as it arrives — using the same
            <code>/ws/subscribe</code> endpoint any browser can use. It is a small
            HTML file and no build step.
            """,
        ),
        (
            "The overlay server is separate on purpose",
            """
            The overlay page is served by its own tiny HTTP server, not by the
            sqtseries gateway. An overlay has to be reachable as a browser source
            on the network, and mixing that into the data gateway would put a
            second, unauthenticated listener on the port that holds your data.
            """,
        ),
        (
            "Connection validation is a real tool",
            """
            <code>conn_validate.py</code> checks the live page end to end: it
            connects, waits for real frames, and reports what it saw. It is how the
            suite proves the page still works after a change.
            """,
        ),
        (
            "The seventeen questions are the point",
            """
            Once a feed is stored it is just time-series data, so every question on
            the <a href="queries.html">Queries</a> page applies. The questions are
            listed with their exact calls on this page.
            """,
        ),
        (
            "Running it yourself",
            """
            The steps below run all three scripts against a live service. The suite
            runs the same path, so a break here is a test failure, not a silent
            drift.
            """,
        ),
    ],
)

page(
    "clients",
    title="sqtseries — Client Libraries: one wire format, six languages",
    description=(
        "Talk to sqtseries from Python, Go, Rust, PHP, Node.js, or the shell. "
        "Same JSON wire format on every transport, no client library required."
    ),
    eyebrow="Runbook · other languages",
    h1="Client Libraries",
    tagline=(
        "There is no client library to install and no schema to generate. If your "
        "language can send JSON over HTTP or ZeroMQ, it already speaks sqtseries. "
        "Copy-paste samples for six languages, and the two rules that matter."
    ),
    meta=[
        ("Rule one", "no IDL, no codegen, no SDK"),
        ("Rule two", "seconds over HTTP, nanoseconds over ZMQ"),
        ("Python", "bundled <code>Client</code>, or embedded"),
        ("Shell", "<code>curl</code> only"),
        ("Go / Rust", "stock ZeroMQ bindings, plain structs"),
    ],
    foot=[
        (
            "Why there is no client library",
            """
            The wire format is small — four fields in, a status envelope out — and
            it is identical on every transport. A generated SDK would add a version
            to keep in step with a format that fits in a paragraph. A hand-written
            struct that matches the shapes on
            <a href="api.html">API Reference</a> is the whole integration.
            """,
        ),
        (
            "The one rule that causes real bugs",
            """
            Timestamps. Over HTTP, <code>start</code> and <code>end</code> are
            <strong>epoch seconds</strong>, and so are the timestamps in the
            response. Over ZeroMQ they are <strong>epoch nanoseconds</strong> in
            the request, and the response is back in seconds. Getting this wrong
            returns an empty result rather than an error, so it is worth stating
            plainly — and it is the first thing to check when a ZMQ query looks
            empty while the HTTP one works.
            """,
        ),
        (
            "Fire and forget, or wait for a reply",
            """
            Ingest is a PUSH socket: there is no reply and no backpressure on the
            client beyond the socket's own queue. Query and admin are REQ: one
            request, one reply, and a timeout that raises rather than hanging
            forever. Python's <code>Client</code> handles both, including closing a
            broken REQ socket so the next call works.
            """,
        ),
        (
            "Embedding instead of connecting",
            """
            A single Python process can skip every socket and open the engine
            in-process — no ports, no service, no serialisation. The same query
            code, the same schema, the same rollup. See the embedded API on
            <a href="api.html#embedded">API Reference</a>.
            """,
        ),
        (
            "Language support, honestly",
            """
            Any language with an HTTP client works today, with no caveats. Any
            language with ZeroMQ bindings gets the higher-rate path. The samples
            here are complete and were checked against the running service.
            """,
        ),
        (
            "When a client misbehaves",
            """
            Check the ordering first, then the port. <code>sqtseries status</code>
            prints the ports the service is <em>actually</em> on, which is not
            always the configured one.
            """,
        ),
    ],
)

page(
    "backup",
    title="sqtseries — Backup, restore and vacuum",
    description=(
        "Back up a sqtseries database with VACUUM INTO, restore it, and "
        "understand what vacuum does and when to run it."
    ),
    eyebrow="Runbook · keeping the data",
    h1="Backup, restore, vacuum",
    tagline=(
        "The whole database is one file, so backing it up is a copy — but the "
        "right copy is a <code>VACUUM INTO</code> snapshot rather than a file copy "
        "of a database that is being written to. Here is the difference, the "
        "procedure, and the restore test."
    ),
    meta=[
        ("Method", "<code>VACUUM INTO</code>, consistent under writes"),
        ("Names", "<code>sqtseries-YYYYmmdd-HHMMSS.db</code>"),
        ("Schedule", "<code>backup.enabled</code>, every <code>24h</code>"),
        ("Overwrite", "never — an existing name is kept"),
        ("Restore", "stop, replace, start, verify"),
    ],
    foot=[
        (
            "Why not just copy the file",
            """
            In WAL mode a live database is three files: the main file, the WAL, and
            the shared-memory file. Copying only the main file mid-write gives you a
            database missing its most recent committed pages.
            <code>VACUUM INTO</code> writes a single, complete, consistent file in
            one pass, and it does not need the service to stop.
            """,
        ),
        (
            "The backup is a full database, not a fragment",
            """
            A snapshot contains the schema, the series table, every partition inside
            the retention window, and the hourly rollup rows for them. Restoring it
            gives back a service that answers historical queries identically,
            without waiting for the rollup to catch up.
            """,
        ),
        (
            "Restore is four steps, and step 4 is the one people skip",
            """
            Stop the service, put the file where <code>database.path</code> points,
            start it, then <em>verify</em> with <code>sqtseries health</code> and a
            count. An unrestored backup is a guess, not a backup. The full procedure,
            including a dry run into a scratch path, is on this page.
            """,
        ),
        (
            "Vacuum is not a backup",
            """
            <code>VACUUM</code> rewrites the file to reclaim free pages. It does not
            protect anything, and it cannot run while another statement holds the
            database. Run <code>sqtseries vacuum</code> with the service stopped for
            anything large; the admin command exists but holds the single SQLite
            writer for the duration, which pauses ingest.
            """,
        ),
        (
            "Automatic backups are on by default",
            """
            The service snapshots once at first start, then every
            <code>backup.interval</code>. A pass that finds a snapshot younger than
            the interval counts the run and writes nothing, so a restart cannot
            produce a second copy in the same second.
            """,
        ),
        (
            "Retention already limits what you need to keep",
            """
            Partitions older than <code>retention.default_ttl</code> are dropped,
            rollup rows and all, so the file does not grow without bound. Choose a
            backup interval and a retention window that suit the data rather than
            archiving indefinitely.
            """,
        ),
    ],
)

page(
    "systemd",
    title="sqtseries — Running as a systemd service",
    description=(
        "Install sqtseries as a hardened systemd unit, per-user or system-wide, "
        "with a dedicated user, correct permissions, and a verified restart."
    ),
    eyebrow="Runbook · operations",
    h1="Systemd",
    tagline=(
        "The service is meant to run for years unattended. This page generates "
        "the unit, explains every hardening directive in it, and shows how to "
        "prove the service comes back after a failure."
    ),
    meta=[
        ("Install", "<code>sqtseries install</code>"),
        ("Scope", "user unit, or <code>--system</code> for root"),
        ("Hardening", "<code>ProtectSystem=strict</code> and more"),
        ("Writable", "the data directory only"),
        ("Restart", "on failure, 5 s backoff"),
    ],
    foot=[
        (
            "Install with the config you actually want",
            """
            The unit embeds the flags it was installed with.
            <code>sqtseries --config c.toml install</code> produces a unit that
            boots with that config, so a service that serves custom ports does not
            silently come back on the defaults after a reboot.
            """,
        ),
        (
            "User unit or system unit",
            """
            Without <code>--system</code> the unit goes to
            <code>~/.config/systemd/user/</code> and needs no root, but it stops
            when you log out unless lingering is enabled. With
            <code>--system</code> it goes to <code>/etc/systemd/system/</code> and
            runs as a system service — for a machine that is always on, this is the
            right choice.
            """,
        ),
        (
            "Hardening, and why each line is there",
            """
            The generated unit sets <code>NoNewPrivileges</code>,
            <code>ProtectSystem=strict</code> and
            <code>ProtectHome=read-only</code>, then re-opens exactly one writable
            path: the data directory. The process therefore cannot write anywhere
            else on the filesystem, and cannot gain privilege through a child
            process. Check it yourself with <code>systemd-analyze security</code>.
            """,
        ),
        (
            "The data directory is the exception",
            """
            Because <code>ProtectHome=read-only</code> also covers
            <code>~</code>, the default database path needs an explicit read-write
            grant or the service will not start. That is the single most common
            systemd failure, and the unit the installer generates already handles
            it — but it is the first thing to check if you hand-write a unit.
            """,
        ),
        (
            "Proving it restarts",
            """
            <code>Restart=on-failure</code> with a 5 s backoff means a crash is
            recovered without help. Kill the process and watch it come back, then
            confirm the data is intact. An untested restart policy is a hope, not a
            plan — the recipe is on this page.
            """,
        ),
        (
            "Stopping is graceful and complete",
            """
            <code>sqtseries stop</code> sends SIGTERM and waits for exit, and the
            service drains the ingest queue, flushes in-flight publishes,
            checkpoints the WAL and removes its runtime file as the last step. Under
            systemd the same happens, so a restart never discards a frame already
            accepted.
            """,
        ),
    ],
)

page(
    "api",
    title="sqtseries — API Reference: routes, commands, error codes",
    description=(
        "The complete sqtseries API: every HTTP route and parameter, every "
        "WebSocket protocol, every admin command, every error code, the wire "
        "format, the embedded Python API, and the CLI."
    ),
    eyebrow="Reference · the surface",
    h1="API Reference",
    tagline=(
        "Everything the service exposes, in one place: the HTTP routes with their "
        "parameters and status codes, the three WebSocket protocols, the admin "
        "socket, the ZeroMQ wire format, the embedded Python API, and the CLI — "
        "with the error code each failure returns."
    ),
    meta=[
        ("HTTP", "7 REST routes, 3 WebSockets, plus docs"),
        ("Admin", "9 commands over REP 12504"),
        ("Schema", "<code>/openapi.json</code> is generated"),
        ("Swagger", "<code>/api-docs</code> and <code>/redoc</code>, self-hosted"),
        ("Errors", "12 codes, named, not stringly typed"),
    ],
    foot=[
        (
            "One shape, three transports",
            """
            An ingest message, a query request and a status envelope are the same
            JSON whether they travel over ZMQ, HTTP or a WebSocket. That is why this
            page can describe a wire format once and it stays true for every client.
            """,
        ),
        (
            "The generated schema is the machine-readable half",
            """
            <code>/openapi.json</code> comes from the FastAPI route definitions, so
            it cannot drift from them. If you generate a client from it, do not
            hand-edit the result — regenerate when the schema changes.
            """,
        ),
        (
            "Errors are codes, not sentences",
            """
            Every failure carries a stable machine-readable <code>code</code> and a
            human <code>message</code>. Match on the code. A client that branches on
            the message text will break when the wording improves.
            """,
        ),
        (
            "Status codes that mean something specific here",
            """
            <code>413</code> is not a payload too large — it is a valid request whose
            <em>result</em> exceeds the bounded-work policy. <code>504</code> is the
            query timeout. A <code>429</code> is the per-peer rate limit, and the
            response says so in a body rather than only a header.
            """,
        ),
        (
            "The embedded API is the same code without the sockets",
            """
            <code>TimeSeriesDB</code> is the query facade the service itself uses. A
            single Python process can construct it directly and skip every port,
            which is the fastest possible integration and the easiest to test.
            """,
        ),
        (
            "Not documented here?",
            """
            The exact defaults live in the source, and the settings reference on
            <a href="configuration.html">Configuration</a> gives each one with its
            key, its variable and its validation rule.
            """,
        ),
    ],
)

page(
    "architecture",
    title="sqtseries — Architecture: the parts and the reasoning",
    description=(
        "How sqtseries is put together: the schema, the write path, the query "
        "path and the rollup fast path, the concurrency model, the background "
        "tasks, and the reasoning behind each."
    ),
    eyebrow="Reference · design",
    h1="Architecture",
    tagline=(
        "One process, one file, six sockets. How a measurement travels from a "
        "frame to a committed row, how a wide query avoids scanning millions of "
        "points, and what bounds memory while all of it runs at once."
    ),
    meta=[
        ("Process", "one, asyncio, SQLite in the same loop"),
        ("Storage", "monthly partitions, WITHOUT ROWID"),
        ("Write", "batched, one transaction per burst"),
        ("Read", "rollup for finished hours, raw for the edges"),
        ("Bounds", "every queue and cache has a stated limit"),
    ],
    foot=[
        (
            "Why one process",
            """
            SQLite allows one writer. Putting the message layer, the query facade
            and the HTTP gateway in the same process removes every inter-process
            serialisation question, and lets a query run against the live engine
            without a second copy of the truth. The cost is that the service is a
            single point of failure — which the systemd unit and the retention and
            backup tasks are there to cover.
            """,
        ),
        (
            "The write path never waits for the disk",
            """
            The receive loop parses frames and hands each batch to a dedicated
            persister through a bounded queue. Awaiting the commit inline would cap
            ingest at the disk's write rate; decoupling it is what lets a sustained
            pump persist far more than that. The bound is the queue, so the
            decoupling cannot become an unbounded buffer.
            """,
        ),
        (
            "Why the hourly summary is exact",
            """
            The rollup stores count, sum, min and max per series per finished hour.
            Every average is then a division of two stored columns, so a
            rollup-served answer is not an estimate of an average — it is the same
            average, computed from sufficient statistics. This is the reason the fast
            path can be trusted for alerting.
            """,
        ),
        (
            "Partitions, not one growing table",
            """
            A monthly table per month means retention is a <code>DROP TABLE</code>
            rather than a mass <code>DELETE</code>, and each table stays small enough
            for the page cache. The cost is a table per month and a partition-name
            lookup on the write path, both cheap.
            """,
        ),
        (
            "Every bound is stated, and every drop is counted",
            """
            Pending batches, fan-out queue bytes, cache entries, cache weight,
            in-flight queries, WebSocket connections, HTTP requests per minute. Each
            has a limit in the settings, and each drop or shed shows up in a
            counter. A bounded system that loses something silently is worse than an
            unbounded one that is honest.
            """,
        ),
        (
            "Read this page when you need to change something",
            """
            <a href="codebase-guide.html">Codebase Guide</a> maps every module to its
            job; this page explains why the design is the way it is.
            """,
        ),
    ],
)

page(
    "codebase",
    title="sqtseries — Codebase Guide: every module and what it is for",
    description=(
        "A map of the sqtseries source tree: every module, its responsibility, "
        "its key classes and functions, and where to start reading."
    ),
    eyebrow="Reference · source map",
    h1="Codebase Guide",
    tagline=(
        "Every file in <code>src/sqtseries/</code>, what it owns, the classes "
        "and functions worth knowing, and the order to read them in if you are "
        "about to change something."
    ),
    meta=[
        ("Source", "<code>src/sqtseries/</code>, {{src_loc}} lines"),
        ("Packages", "engine, messaging, query, partition, gateway"),
        ("Entry point", "<code>sqtseries.cli:main</code>"),
        ("Tests", "<code>tests/</code>, run with <code>pytest</code>"),
        ("Read order", "service, then the layer you are changing"),
    ],
    foot=[
        (
            "Start at the service",
            """
            <code>service.py</code> is the only place that knows the whole start-up
            order, which sockets bind in what order, and what happens on shutdown.
            Every other module can be read in isolation; this one cannot, and it is
            the right place to start.
            """,
        ),
        (
            "Packages divide by concern, not by layer",
            """
            <code>engine</code> owns the file, <code>partition</code> owns time
            buckets, <code>query</code> owns reading, <code>messaging</code> owns the
            sockets, <code>gateway</code> owns HTTP. A change usually lives in
            exactly one of them.
            """,
        ),
        (
            "Comments carry measurements, not moods",
            """
            The source explains <em>why</em> a constant is what it is, and cites the
            run that established it. When you change a bound, update the comment with
            it or delete it — a stale justification is worse than none.
            """,
        ),
        (
            "The test suite is the specification",
            """
            The claims these docs make are backed by named tests, and the suite is
            the place to look before changing behaviour.
            <code>tests/test_docs_claims.py</code> exists specifically to keep this
            documentation honest.
            """,
        ),
        (
            "Conventions worth matching",
            """
            Type hints on every public function. Errors are typed and carry a stable
            code. Nothing is swallowed silently — a dropped batch, a shed request
            and a failed backup each increment a counter. Comments on their own
            line, and only where the reasoning is not obvious from the code.
            """,
        ),
        (
            "Where the boundaries are not",
            """
            The engine has no idea the network exists, and the query layer has no
            idea about sockets. If you find yourself importing across those lines,
            that is a signal about where the change belongs.
            """,
        ),
    ],
)

page(
    "examples",
    title="sqtseries — Examples: the scripts, run and shown",
    description=(
        "Runnable sqtseries examples: server monitoring, dashboards, alerting, "
        "retention, high-rate ingest, health checks, and the four canonical "
        "questions."
    ),
    eyebrow="Runbook · worked examples",
    h1="Examples",
    tagline=(
        "Every example script in the repository, what it does, the exact command "
        "to run it, and the output to expect — plus four worked questions with the "
        "real calls that answer them."
    ),
    meta=[
        ("Scripts", "17, every one runnable"),
        ("Runner", "<code>run_all_examples.py</code> runs them all"),
        ("Coverage", "a test runs the suite against a live service"),
        ("Questions", "4 canonical, with exact calls"),
        ("Camera", "17 questions answered with real calls"),
    ],
    foot=[
        (
            "These are real scripts, not snippets",
            """
            Each one lives in <code>examples/</code>, runs against a live service,
            and is executed by the test suite. If a script breaks, a test fails — so
            a sample that no longer works cannot sit here quietly.
            """,
        ),
        (
            "Run one, or run them all",
            """
            Every script takes <code>--help</code> and documents its own options.
            <code>run_all_examples.py</code> starts a service on scratch ports, runs
            the whole set, and tears it down, which is the fastest way to see what
            any of them does.
            """,
        ),
        (
            "The four canonical questions",
            """
            Average CPU load over the last hour; peak temperature yesterday; p99
            response time over the past week; how many measurements were recorded
            today. Four questions, four exact calls, four explanations of why each
            is shaped the way it is.
            """,
        ),
        (
            "Two examples are about the service, not the data",
            """
            The connection monitor and the load-balancer health check talk to the
            admin and health surfaces rather than storing anything. They are here
            because “is it up” and “is it busy” are the questions you ask before
            any of the others.
            """,
        ),
        (
            "The scripts are the fastest way to a working integration",
            """
            Pick the one closest to what you are building, read it, and change the
            metric names. That is usually less work than starting from the API
            reference.
            """,
        ),
        (
            "Checked against the running service",
            """
            A test boots a service, runs the examples, and asserts the output. That
            is why the numbers on this page are measured rather than remembered.
            """,
        ),
    ],
)

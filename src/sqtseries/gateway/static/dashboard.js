/* sqtseries admin dashboard client. Vanilla JS, no dependencies.
   One WebSocket (/ws/dashboard): snapshot on connect, live conn/sub
   events, 1s counter ticks. Leak safety: single socket, one UI timer,
   bounded ticker/table buffers, full teardown on pagehide. No console
   output on any path — failures render into the DOM. */
(function () {
  "use strict";

  var MAX_TICKER = 50;
  var MAX_ROWS = 200;
  var PAGE_SIZE = 5;
  var MAX_BACKOFF_MS = 10000;
  var REFRESH_DEBOUNCE_MS = 150;

  var els = {};
  var ws = null;
  var backoffMs = 1000;
  var backoffTimer = null;
  var ageTimer = null;
  var prevCounters = null;
  var prevTickAt = 0;
  var refreshTimer = null;
  var refreshGeneration = 0;
  var topicView = { rows: [], page: 0, q: "" };
  var connView = { rows: [], page: 0, q: "" };

  function el(id) {
    if (!els[id]) {
      els[id] = document.getElementById(id);
    }
    return els[id];
  }

  function setText(id, value) {
    var node = el(id);
    if (node && node.textContent !== value) {
      node.textContent = value;
    }
  }

  function fmtInt(n) {
    if (n === null || n === undefined) {
      return "—";
    }
    return Number(n).toLocaleString("en-US");
  }

  function fmtBytes(n) {
    if (n === null || n === undefined) {
      return "—";
    }
    var units = ["B", "KB", "MB", "GB"];
    var i = 0;
    var v = Number(n);
    while (v >= 1024 && i < units.length - 1) {
      v /= 1024;
      i += 1;
    }
    return v.toFixed(1) + " " + units[i];
  }

  function fmtAge(ts) {
    if (!ts) {
      return "—";
    }
    var s = Math.max(0, Math.floor(Date.now() / 1000 - ts));
    return s + "s ago";
  }

  function fmtDuration(totalSeconds) {
    if (totalSeconds === null || totalSeconds === undefined) {
      return "—";
    }
    var s = Math.max(0, Math.floor(totalSeconds));
    var parts = [];
    var h = Math.floor(s / 3600);
    var m = Math.floor((s % 3600) / 60);
    if (h) {
      parts.push(h + "h");
    }
    if (m || h) {
      parts.push(m + "m");
    }
    parts.push((s % 60) + "s");
    return parts.join(" ");
  }

  function fmtHourNs(ns) {
    if (!ns) {
      return "—";
    }
    return new Date(Math.floor(ns / 1000000)).toLocaleString("en-GB");
  }

  function setPill(connected) {
    var pill = el("conn-pill");
    var label = el("conn-state-text");
    if (!pill || !label) {
      return;
    }
    pill.classList.toggle("connected", connected);
    pill.classList.toggle("reconnecting", !connected);
    label.textContent = connected ? "connected" : "reconnecting";
  }

  function tickRow(text, kind) {
    var list = el("ticker");
    if (!list) {
      return;
    }
    var li = document.createElement("li");
    li.textContent = new Date().toLocaleTimeString("en-GB") + "  " + text;
    if (kind) {
      li.className = "t-" + kind;
    }
    list.insertBefore(li, list.firstChild);
    while (list.children.length > MAX_TICKER) {
      list.removeChild(list.lastChild);
    }
  }

  function renderStrip(snap) {
    var dot = el("status-dot");
    if (dot) {
      dot.className =
        "status-dot " + (snap.status === "ok" ? "ok" : "degraded");
    }
    setText("stat-status", snap.status || "—");
    setText("stat-uptime", fmtDuration(snap.uptime_s));
    setText("stat-version", snap.version || "—");
    setText("stat-db", snap.db_path || "—");
    setText("stat-db-bytes", fmtBytes(snap.db_bytes));
    var ports = snap.ports || {};
    var names = Object.keys(ports).sort();
    setText(
      "stat-ports",
      names
        .map(function (k) {
          return k + ":" + ports[k];
        })
        .join("  "),
    );
    setText("stat-wal", fmtBytes(snap.wal_bytes));
    setText(
      "stat-checkpoints",
      fmtInt(snap.checkpoints) +
        " (" +
        fmtInt(snap.checkpoint_busy_runs) +
        " busy)",
    );
  }

  function renderRates(tick) {
    var now = Date.now() / 1000;
    if (prevCounters && now > prevTickAt) {
      var dt = now - prevTickAt;
      var rate = function (key) {
        var cur = tick[key];
        var prev = prevCounters[key];
        if (cur === undefined || prev === undefined) {
          return "—";
        }
        return ((cur - prev) / dt).toFixed(1) + "/s";
      };
      setText("stat-ingest-rate", rate("ingested"));
      setText("stat-query-rate", rate("queries"));
      setText("stat-publish-rate", rate("published"));
    }
    prevCounters = {
      ingested: tick.ingested,
      queries: tick.queries,
      published: tick.published,
    };
    prevTickAt = now;
    setText("stat-ingested", fmtInt(tick.ingested));
    setText("stat-invalid", fmtInt(tick.invalid));
    setText("stat-ingest-errors", fmtInt(tick.ingest_errors));
    setText("stat-persisted", fmtInt(tick.persisted || 0));
    setText("stat-dropped-points", fmtInt(tick.dropped || 0));
    setText("stat-queries", fmtInt(tick.queries));
    var hits = tick.query_cache_hits || 0;
    var misses = tick.query_cache_misses || 0;
    var total = hits + misses;
    setText(
      "stat-cache-ratio",
      total ? ((100 * hits) / total).toFixed(1) + "%" : "—",
    );
    setText("stat-cache-size", fmtInt(tick.query_cache_size) + " / 512");
    setText("stat-published", fmtInt(tick.published));
  }

  function matchQuery(q, values) {
    if (!q) {
      return true;
    }
    var needle = q.toLowerCase();
    for (var i = 0; i < values.length; i += 1) {
      var v = values[i];
      if (
        v !== undefined &&
        v !== null &&
        String(v).toLowerCase().indexOf(needle) !== -1
      ) {
        return true;
      }
    }
    return false;
  }

  function renderPage(view, total) {
    var pages = Math.max(1, Math.ceil(total / PAGE_SIZE));
    if (view.page > pages - 1) {
      view.page = pages - 1;
    }
    if (view.page < 0) {
      view.page = 0;
    }
    return {
      page: view.page,
      pages: pages,
      start: view.page * PAGE_SIZE,
    };
  }

  function paintPager(prefix, view, total) {
    var info = renderPage(view, total);
    var from = total === 0 ? 0 : info.start + 1;
    var to = Math.min(total, info.start + PAGE_SIZE);
    setText(
      prefix + "-page-info",
      from + "–" + to + " of " + total + " · page " + (info.page + 1) +
        " of " + info.pages,
    );
    var prev = el(prefix + "-prev");
    var next = el(prefix + "-next");
    if (prev) {
      prev.disabled = info.page === 0;
    }
    if (next) {
      next.disabled = info.page >= info.pages - 1;
    }
    return info;
  }

  function renderConnections(list) {
    var body = el("conn-rows");
    var note = el("conn-note");
    if (!body) {
      return;
    }
    while (body.firstChild) {
      body.removeChild(body.firstChild);
    }
    connView.rows = list.slice();
    var rows = connView.rows
      .slice()
      .reverse()
      .filter(function (c) {
        return matchQuery(connView.q, [c.id, c.peer, c.topic]);
      });
    var info = paintPager("conn", connView, rows.length);
    var frag = document.createDocumentFragment();
    rows.slice(info.start, info.start + PAGE_SIZE).forEach(function (c) {
      var tr = document.createElement("tr");
      ["id", "peer", "topic"].forEach(function (k) {
        var td = document.createElement("td");
        td.textContent =
          c[k] === undefined || c[k] === null ? "—" : String(c[k]);
        tr.appendChild(td);
      });
      var age = document.createElement("td");
      age.textContent = fmtAge(c.last_activity_at || c.connected_at);
      age.setAttribute("data-ts", c.last_activity_at || c.connected_at || "");
      tr.appendChild(age);
      var stale = document.createElement("td");
      var isStale =
        c.last_activity_at && Date.now() / 1000 - c.last_activity_at >= 30;
      stale.textContent = isStale ? "stale" : "live";
      if (isStale) {
        stale.className = "stale-flag";
      }
      tr.appendChild(stale);
      frag.appendChild(tr);
    });
    body.appendChild(frag);
    setText("stat-connections", fmtInt(list.length));
    if (note) {
      note.textContent = fmtInt(list.length) + " active";
    }
  }

  function renderTopics(subs) {
    var body = el("topic-rows");
    if (!body) {
      return;
    }
    while (body.firstChild) {
      body.removeChild(body.firstChild);
    }
    topicView.rows = subs.slice();
    var rows = topicView.rows
      .slice()
      .sort(function (a, b) {
        var fa =
          a.first_seen === undefined || a.first_seen === null ? -1 : a.first_seen;
        var fb =
          b.first_seen === undefined || b.first_seen === null ? -1 : b.first_seen;
        return fb - fa;
      })
      .filter(function (s) {
        return matchQuery(topicView.q, [s.topic]);
      });
    var info = paintPager("topic", topicView, rows.length);
    var frag = document.createDocumentFragment();
    rows.slice(info.start, info.start + PAGE_SIZE).forEach(function (s) {
      var tr = document.createElement("tr");
      var topicCell = document.createElement("td");
      topicCell.textContent =
        s.topic === undefined || s.topic === null || s.topic === ""
          ? "(all)"
          : String(s.topic);
      tr.appendChild(topicCell);
      var countCell = document.createElement("td");
      countCell.textContent =
        s.subscribers === undefined || s.subscribers === null
          ? "0"
          : String(s.subscribers);
      tr.appendChild(countCell);
      var totalCell = document.createElement("td");
      totalCell.textContent =
        s.total === undefined || s.total === null ? "—" : fmtInt(s.total);
      tr.appendChild(totalCell);
      frag.appendChild(tr);
    });
    body.appendChild(frag);
    var total = subs.reduce(function (acc, s) {
      return acc + (s.subscribers || 0);
    }, 0);
    setText("stat-subscribers", fmtInt(total));
  }

  function renderStorage(snap) {
    setText("stat-series", fmtInt(snap.series));
    setText("stat-metrics", fmtInt(snap.metrics));
    setText("stat-partitions", fmtInt(snap.partitions));
    setText("stat-dropped", fmtInt(snap.partitions_dropped));
    setText("stat-backup", snap.last_backup || "never");
    setText("stat-maint-runs", fmtInt(snap.analyze_runs));
    setText("stat-retention-runs", fmtInt(snap.retention_runs));
    setText("stat-backup-runs", fmtInt(snap.backup_runs));
    setText(
      "stat-watermark",
      typeof snap.rollup_watermark === "number"
        ? fmtHourNs(snap.rollup_watermark)
        : "—",
    );
  }

  function renderSnapshot(snap) {
    renderStrip(snap);
    renderRates(snap);
    renderConnections(snap.connections || []);
    renderTopics(snap.topics || snap.subscriptions || []);
    renderStorage(snap);
    tickRow("snapshot received", "tick");
  }

  function applyEvent(msg) {
    if (msg.type === "conn") {
      tickRow(
        "conn " +
          (msg.id || "?") +
          " " +
          (msg.connected === false ? "left" : "joined"),
        "conn",
      );
      refreshLists();
    } else if (msg.type === "sub") {
      tickRow(
        "sub " + (msg.topic || "?") + " → " + (msg.subscribers || 0),
        "sub",
      );
      refreshLists();
    }
  }

  /* Conn/sub events arrive in bursts (e.g. mass connect under load).
     Coalesce them into one refresh per debounce window; a generation
     counter drops stale responses that resolve out of order. */
  function refreshLists() {
    if (refreshTimer !== null) {
      return;
    }
    refreshTimer = window.setTimeout(function () {
      refreshTimer = null;
      fetchLists();
    }, REFRESH_DEBOUNCE_MS);
  }

  function fetchLists() {
    refreshGeneration += 1;
    var gen = refreshGeneration;
    fetch("/api/v1/connections", { cache: "no-store" })
      .then(function (r) {
        return r.json();
      })
      .then(function (body) {
        if (gen === refreshGeneration) {
          renderConnections(body.data || []);
        }
      })
      .catch(function () {});
    fetch("/api/v1/subscribers", { cache: "no-store" })
      .then(function (r) {
        return r.json();
      })
      .then(function (body) {
        if (gen === refreshGeneration) {
          renderTopics(body.topics || body.subscriptions || []);
        }
      })
      .catch(function () {});
  }

  function refreshAges() {
    var nodes = document.querySelectorAll("[data-ts]");
    for (var i = 0; i < nodes.length; i += 1) {
      var ts = parseFloat(nodes[i].getAttribute("data-ts"));
      if (ts) {
        nodes[i].textContent = fmtAge(ts);
      }
    }
    var feed = el("feed-age");
    if (feed && prevTickAt) {
      feed.textContent = "feed " + fmtAge(prevTickAt);
    }
  }

  function scheduleReconnect() {
    if (backoffTimer !== null) {
      return;
    }
    backoffTimer = window.setTimeout(function () {
      backoffTimer = null;
      connect();
    }, backoffMs);
    backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
  }

  function connect() {
    if (ws !== null) {
      return;
    }
    var proto = window.location.protocol === "https:" ? "wss://" : "ws://";
    var sock;
    try {
      sock = new WebSocket(proto + window.location.host + "/ws/dashboard");
    } catch (e) {
      scheduleReconnect();
      return;
    }
    ws = sock;
    sock.onopen = function () {
      backoffMs = 1000;
      setPill(true);
    };
    sock.onmessage = function (ev) {
      var msg;
      try {
        msg = JSON.parse(ev.data);
      } catch (e) {
        return;
      }
      if (!msg || !msg.type) {
        return;
      }
      if (msg.type === "snapshot" || msg.type === "tick") {
        if (msg.type === "snapshot") {
          renderSnapshot(msg);
        } else {
          renderStrip(msg);
          renderRates(msg);
          renderStorage(msg);
          if (msg.topics) {
            renderTopics(msg.topics);
          }
        }
      } else {
        applyEvent(msg);
      }
    };
    var done = function () {
      if (ws === sock) {
        ws = null;
      }
      setPill(false);
      try {
        sock.close();
      } catch (e) {}
      scheduleReconnect();
    };
    sock.onclose = done;
    sock.onerror = function () {
      try {
        sock.close();
      } catch (e) {}
    };
  }

  function teardown() {
    if (backoffTimer !== null) {
      window.clearTimeout(backoffTimer);
      backoffTimer = null;
    }
    if (refreshTimer !== null) {
      window.clearTimeout(refreshTimer);
      refreshTimer = null;
    }
    refreshGeneration += 1;
    if (ageTimer !== null) {
      window.clearInterval(ageTimer);
      ageTimer = null;
    }
    if (ws !== null) {
      try {
        ws.onclose = null;
        ws.close();
      } catch (e) {}
      ws = null;
    }
    els = {};
  }

  function wirePager(prefix, view, rerender) {
    var search = el(prefix + "-search");
    if (search && !search.dataset.wired) {
      search.dataset.wired = "1";
      search.addEventListener("input", function () {
        view.q = search.value;
        view.page = 0;
        rerender();
      });
    }
    var prev = el(prefix + "-prev");
    if (prev && !prev.dataset.wired) {
      prev.dataset.wired = "1";
      prev.addEventListener("click", function () {
        if (view.page > 0) {
          view.page -= 1;
          rerender();
        }
      });
    }
    var next = el(prefix + "-next");
    if (next && !next.dataset.wired) {
      next.dataset.wired = "1";
      next.addEventListener("click", function () {
        view.page += 1;
        rerender();
      });
    }
  }

  function rerenderTopics() {
    renderTopics(topicView.rows);
  }

  function rerenderConns() {
    renderConnections(connView.rows);
  }

  function init() {
    if (!("WebSocket" in window)) {
      setPill(false);
      setText("conn-state-text", "unsupported");
      return;
    }
    wirePager("topic", topicView, rerenderTopics);
    wirePager("conn", connView, rerenderConns);
    ageTimer = window.setInterval(refreshAges, 1000);
    window.addEventListener("pagehide", teardown);
    connect();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

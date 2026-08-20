# Connection & Disconnection Test Scenarios — Reusable Guide

This document catalogs every test scenario validated in the sqtseries project's
connection tracking system. Another LLM can pick this up and apply it to any
project that manages WebSocket connections, ZMQ subscribers, or similar
connection/disconnection lifecycle tracking.

---

## Table of Contents

1. [Architecture Context](#1-architecture-context)
2. [What Was Tested](#2-what-was-tested)
3. [Scenario Catalog](#3-scenario-catalog)
4. [Leak Detection Methodology](#4-leak-detection-methodology)
5. [Bug Found & Fixed During Testing](#5-bug-found--fixed-during-testing)
6. [How to Apply This to Another Project](#6-how-to-apply-this-to-another-project)

---

## 1. Architecture Context

The system under test is a **ConnectionRegistry** — a central in-memory tracker
for WebSocket connections and ZMQ PUB/SUB subscribers. It:

- Tracks WS connections by unique ID (peer address, topic, timestamps)
- Tracks ZMQ SUB subscribers by topic (count, first_seen timestamps)
- Emits `conn` and `sub` events on every state change
- Supports listener registration/removal for external consumers (stats publisher)
- Exposes snapshots and queries for admin dashboards

The registry is single-threaded (called from an asyncio event loop). All
mutation methods are synchronous. Event emission is synchronous with listener
snapshots to prevent reentrancy bugs.

---

## 2. What Was Tested

**Total tests:** 112 (103 in `test_connection_tracking.py`, 9 in `test_dafka_adopted.py`)
**Leak analysis:** tracemalloc + gc.get_objects() across 25,000 register/unregister cycles
**Result:** +35 objects (noise), zero leaks

---

## 3. Scenario Catalog

### Category A: WebSocket Connection Lifecycle

| # | Scenario | What It Proves | Priority |
|---|----------|---------------|----------|
| A1 | Register then unregister | Basic lifecycle works | Critical |
| A2 | Unregister unknown ID (idempotent) | No crash on double-free | Critical |
| A3 | Register same ID twice | Overwrites previous entry, no duplication | High |
| A4 | Unregister then re-register same ID | Clean re-entry after departure | High |
| A5 | Register with empty string conn_id | Edge: empty ID is valid | Medium |
| A6 | Register with empty string topic | Edge: empty topic accepted | Medium |
| A7 | Register with empty string peer | Edge: empty peer accepted | Medium |
| A8 | Register with Unicode topic | Edge: non-ASCII strings work | Medium |
| A9 | Register with 10,000-char topic | Edge: long strings accepted | Low |
| A10 | 1000 concurrent connections | Scale: no accumulation | High |
| A11 | 5000 rapid connect/disconnect cycles | Scale: no drift or leak | High |
| A12 | Full lifecycle: register → touch → unregister → register → unregister | State transitions are clean | High |
| A13 | check_connection returns True only while registered | State query accuracy | Medium |
| A14 | check_connection after overwrite still True | Overwrite preserves liveness | Medium |
| A15 | connected_at immutable across multiple reads | Timestamp stability | Medium |
| A16 | list_connections sorted by connected_at | Ordering correctness | Medium |
| A17 | list_connections returns copies (mutation safety) | Mutating returned dict doesn't leak into registry | Critical |
| A18 | ws_count property reflects real-time state | No cached/stale count | Medium |

### Category B: ZMQ Subscriber Tracking

| # | Scenario | What It Proves | Priority |
|---|----------|---------------|----------|
| B1 | Register then unregister | Basic count increment/decrement | Critical |
| B2 | Unsubscribe more times than subscribed | Count clamps to 0, never negative | Critical |
| B3 | 1000 rapid subscribe/unsubscribe cycles | No count drift | High |
| B4 | Multiple topics independent counts | Per-topic isolation | High |
| B5 | Subscribe → unsubscribe → subscribe | first_seen resets on fresh cycle | High |
| B6 | Subscribe → subscribe → unsubscribe | first_seen preserved when count > 0 | High |
| B7 | Unsubscribe unknown topic | Emits event with count=0, first_seen=None | Medium |
| B8 | 1000 subscribers on same topic | Count accuracy at scale | High |
| B9 | subscriber_count(None) returns total | API: None = all topics | Medium |
| B10 | subscriber_count("") returns total | API: empty string is falsy → total | Medium (quirk) |
| B11 | subscriber_count(0) returns total | API: 0 is falsy → total | Low (quirk) |
| B12 | active_topics returns sorted list | Ordering correctness | Medium |
| B13 | active_topics empty when no subs | Empty state | Low |
| B14 | zmq_sub_count property reflects real-time state | No cached count | Medium |

### Category C: Event Emission & Listener Lifecycle

| # | Scenario | What It Proves | Priority |
|---|----------|---------------|----------|
| C1 | Single listener receives all events | Basic callback works | Critical |
| C2 | Multiple listeners all receive events | Fan-out works | High |
| C3 | Listener exception doesn't break other listeners | Exception isolation (suppress) | Critical |
| C4 | Listener removes itself during emit | No skip of next listener (snapshot) | Critical |
| C5 | Listener removes a different listener during emit | Removed listener still fires on current event (snapshot) | High |
| C6 | Listener adds a new listener during emit | New listener doesn't fire on current event | High |
| C7 | Same callback registered twice | Fires twice per event | Medium |
| C8 | remove_listener removes only first occurrence | Duplicate handling | Medium |
| C9 | remove_listener called twice removes both copies | Full cleanup | Medium |
| C10 | Remove listener never added (idempotent) | No crash | Critical |
| C11 | Emit with zero listeners | No-op, no crash | Medium |
| C12 | remove_listener during emit is safe | List snapshot prevents corruption | Critical |

### Category D: Data Integrity

| # | Scenario | What It Proves | Priority |
|---|----------|---------------|----------|
| D1 | WS connect event has all fields | Payload completeness (kind, id, peer, topic, connected, connected_at, ttl) | High |
| D2 | WS disconnect event has all fields | Payload completeness (+ left_at, connected_at preserved) | High |
| D3 | ZMQ subscribe event has all fields | Payload completeness (kind, topic, subscribers, arrived_at, ttl) | High |
| D4 | ZMQ unsubscribe event has all fields | Payload completeness (+ left_at, first_seen) | High |
| D5 | WS connect event ttl=60 | Correct TTL value | Medium |
| D6 | ZMQ subscribe event ttl=30 | Correct TTL value | Medium |
| D7 | list_connections returns copies not references | Mutation safety | Critical |
| D8 | snapshot returns copies not references | Mutation safety | Critical |
| D9 | snapshot reflects state at call time | Not cached/stale | Medium |
| D10 | snapshot of empty registry returns zeroed counts | Empty state correctness | Low |

### Category E: Cross-Cutting Concerns

| # | Scenario | What It Proves | Priority |
|---|----------|---------------|----------|
| E1 | WS and ZMQ events don't cross-contaminate | Independent event types | High |
| E2 | ZMQ events don't affect ws_count | Independent counters | High |
| E3 | WS events don't affect zmq_sub_count | Independent counters | High |
| E4 | Multiple registries are isolated | No shared state | High |

### Category F: SubscriptionTracker (Direct)

| # | Scenario | What It Proves | Priority |
|---|----------|---------------|----------|
| F1 | Subscribe → unsubscribe → re-subscribe clears linger | Linger lifecycle | High |
| F2 | Unsubscribe creates lingering entry | Linger persistence | High |
| F3 | Linger expires after linger_seconds | Cleanup works | Medium |
| F4 | Double subscribe → active_topics shows once | Set-based, not counter | Medium |
| F5 | Unsubscribe unknown topic | No crash, lingers even if never active | Medium |

### Category G: StatsPublisher (Event Forwarding)

| # | Scenario | What It Proves | Priority |
|---|----------|---------------|----------|
| G1 | Start/stop basic lifecycle | Socket creation and cleanup | Critical |
| G2 | Publish after stop is no-op | No crash on closed socket | Critical |
| G3 | Event hook fires on registry changes | Events forwarded to PUB socket | High |
| G4 | Event hook after stop is no-op | No tasks spawned after shutdown | High |
| G5 | 5 start/stop cycles → 0 listeners | No listener leak | Critical |
| G6 | Report contains uptime > 0 | Periodic report works | Medium |

### Category H: Connection ID Generation

| # | Scenario | What It Proves | Priority |
|---|----------|---------------|----------|
| H1 | 10,000 generated IDs are all unique | Collision resistance | High |
| H2 | ID is 12-char hex string | Format correctness | Medium |

---

## 4. Leak Detection Methodology

### What Was Used

```python
import tracemalloc
import gc

tracemalloc.start()
gc.collect()
baseline = len(gc.get_objects())
snap1 = tracemalloc.take_snapshot()

# ... run 25,000 operations ...

gc.collect()
after = len(gc.get_objects())
snap2 = tracemalloc.take_snapshot()

# Compare
top = snap2.compare_to(snap1, 'lineno')
for stat in top[:10]:
    print(stat)
```

### Stress Test Phases

1. **WS churn:** 10,000 register/unregister cycles (each with touch_ws)
2. **ZMQ churn:** 10,000 subscribe/unsubscribe cycles
3. **Mixed churn:** 5,000 combined WS+ZMQ cycles
4. **Listener churn:** 1,000 add/remove cycles
5. **StatsPublisher churn:** 20 start/stop cycles
6. **Object count:** gc.get_objects() before and after
7. **tracemalloc diff:** Top allocators by byte count and object count

### What "No Leaks" Means

- Object count delta < 100 after 60,000 events
- `StatsPublisher` instances = 0 after all cycles
- `Listeners` count = 1 (the counter used during testing)
- `WS count` = 0, `ZMQ count` = 0
- Top allocators are framework internals (structlog, asyncio, zmq), not application code

### What We Found

```
Baseline: 39,398 objects
After 25k operations + 20 pub cycles: 39,433 objects
Total delta: +35 objects (noise)
```

The +35 is normal Python runtime noise (interned strings, frame objects).

### py-spy Note

py-spy 0.4.2 cannot attach to Python 3.14 (version incompatibility — "Failed to
find python version from target process"). Use `tracemalloc` + `gc` instead for
Python 3.14+. For older Python versions, py-spy works well:

```bash
# Start py-spy watcher in tmux
tmux new -d -s pyspy "bash scripts/py-spy-watch.sh 1 /tmp/py-spy.log"

# Run tests in another terminal
python3 -m pytest tests/ -q

# Analyze
grep "dump pid=" /tmp/py-spy.log | wc -l
# Look for: stuck stacks, growing threads, deadlocks
```

---

## 5. Bug Found & Fixed During Testing

### `_emit()` Listener Snapshot Bug

**Problem:** `_emit()` iterated `self._listeners` in-place. If a listener
removed itself during emission, the next listener in the list was skipped
because Python list iteration shifts indices when an element is removed.

**Test that caught it:**
```python
def test_remove_listener_during_emit(self):
    events = []
    reg = ConnectionRegistry()

    def remover(etype, payload):
        reg.remove_listener(remover)

    reg.on_event(remover)
    reg.on_event(lambda et, p: events.append(et))

    reg.register_ws("x", "peer", "t")  # remover fires, removes itself
    reg.unregister_ws("x")             # remover NOT called (skipped!)
    assert len(events) == 2  # FAILED: only 1 event received
```

**Root cause:**
```python
# BEFORE (buggy)
def _emit(self, event_type, payload):
    for cb in self._listeners:       # iterating live list
        cb(event_type, payload)       # listener removes itself mid-iteration
                                       # → next listener skipped
```

**Fix:**
```python
# AFTER (correct)
def _emit(self, event_type, payload):
    for cb in list(self._listeners):  # snapshot the list
        cb(event_type, payload)        # removals don't affect iteration
```

**Lesson:** Always snapshot a collection before iterating if listeners/callbacks
can modify it during iteration.

---

## 6. How to Apply This to Another Project

### Step 1: Identify Your Connection Registry

Find the class/module that tracks connections. Look for:
- `register_*` / `unregister_*` methods
- `on_event` / `emit` / callback patterns
- `snapshot` / `list_*` / `count` queries
- Timestamp tracking (`connected_at`, `last_activity_at`)

### Step 2: Map Scenarios to Your Code

Replace the generic names with your actual method names:

| Generic | Your Code |
|---------|-----------|
| `register_ws(conn_id, peer, topic)` | `your_register_method(...)` |
| `unregister_ws(conn_id)` | `your_unregister_method(...)` |
| `touch_ws(conn_id)` | `your_activity_update(...)` |
| `subscriber_count(topic)` | `your_count_query(...)` |
| `on_event(callback)` | `your_event_registration(...)` |
| `snapshot()` | `your_snapshot_method(...)` |

### Step 3: Write Tests by Category

Start with the critical priority scenarios (A1-A2, B1-B2, C1-C3, D7-D8) and
work your way down. The categories are ordered by importance:

1. **Lifecycle basics** (A1-A2, B1) — does register/unregister work?
2. **Edge cases** (A3-A9, B2, B7) — what happens with bad inputs?
3. **Scale** (A10-A11, B8) — does it hold up under load?
4. **Event integrity** (C1-C6, D1-D6) — are events emitted correctly?
5. **Data safety** (D7-D8) — can callers corrupt internal state?
6. **Cross-cutting** (E1-E4) — do different subsystems interfere?
7. **Memory leaks** (Section 4) — does it hold memory after churn?

### Step 4: Run Leak Detection

```python
import tracemalloc, gc

def leak_check(label, setup_fn, churn_fn, teardown_fn, iterations=10000):
    tracemalloc.start()
    gc.collect()
    baseline = len(gc.get_objects())

    obj = setup_fn()
    for _ in range(iterations):
        churn_fn(obj)
    teardown_fn(obj)
    gc.collect()

    delta = len(gc.get_objects()) - baseline
    status = "✅" if delta < 100 else "⚠️"
    print(f"{status} {label}: {delta} objects after {iterations} iterations")
```

### Step 5: Common Pitfalls to Test

1. **Double-free:** Unregister something that's already unregistered
2. **Overwrite:** Register the same ID twice
3. **Counter underflow:** Unsubscribe more than subscribed
4. **Listener leak:** Register listeners without removing them
5. **Shallow copy leak:** Returning mutable references to internal state
6. **Snapshot staleness:** Returning cached vs live data
7. **Reentrancy:** Listener modifies the collection during iteration
8. **Event payload completeness:** Every field present with correct type
9. **TTL values:** Events carry correct time-to-live
10. **Cross-subsystem isolation:** Different counters don't interfere

---

## File Locations

| File | Purpose |
|------|---------|
| `tests/test_connection_tracking.py` | All 103 connection tracking tests |
| `tests/test_dafka_adopted.py` | Query cache tests (9 tests, related) |
| `src/sqtseries/messaging/connection_registry.py` | Registry under test |
| `src/sqtseries/messaging/stats_publisher.py` | Event forwarding (tested) |
| `src/sqtseries/messaging/pubsub.py` | SubscriptionTracker (tested) |
| `src/sqtseries/gateway/websocket.py` | WS handler (uses registry) |
| `markdown/TEST_SCENARIOS_GUIDE.md` | This file |

# Pushpin PR proposals (from sqtseries ZeroMQ hardening)

Source of every claim: `/home/iam/devcode/zone/pushpin/src` (read 2026-09-06).
Companion work: sqtseries `src/sqtseries/messaging/pubsub.py` heartbeat diff
(`HEARTBEAT_IVL=1000` / `TIMEOUT=5000` / `TTL=5000` + `MAXMSGSIZE=50MB`) and
`markdown/RESEARCHES.md` § Connection-scale hardening.

## The sqtseries diff under review

```diff
--- b/src/sqtseries/messaging/pubsub.py
+++ b/src/sqtseries/messaging/pubsub.py
@@ -98,6 +98,11 @@ class PubSub:
         self.socket.immediate = 1
         self.socket.setsockopt(zmq.LINGER, 500)
         self.socket.setsockopt(zmq.SNDHWM, self.hwm)
+        # Dead peers (no unsubscribe frame) evict after 5s idle.
+        self.socket.setsockopt(zmq.HEARTBEAT_IVL, 1000)
+        self.socket.setsockopt(zmq.HEARTBEAT_TIMEOUT, 5000)
+        self.socket.setsockopt(zmq.HEARTBEAT_TTL, 5000)
+        self.socket.setsockopt(zmq.MAXMSGSIZE, 50 * 1024 * 1024)
```

Why it was needed: libzmq only tears down silent peers via heartbeat
timers (`stream_engine_base.cpp:741-749`, defaults off). Proven in sqtseries:
SIGKILL'd subscriber evicts via TCP close; SIGSTOP-frozen subscriber (no FIN,
no pong) evicts after 6.0s; 3000 concurrent SUBs tracked exactly.

## Pushpin code paths checked

| Path | Role | Note |
|------|------|------|
| `connmgr/zhttpsocket.rs:2059-2066` | Production sockets: ROUTER + PULL + ROUTER + **PUB** (fan-out) | HWM set on some (`:2068-2089`), no heartbeat anywhere |
| `connmgr/client.rs:2187-2199` | DEALER + PUSH + ROUTER + **SUB** | Same: no heartbeat, no reconnect cap |
| `connmgr/server.rs:1827-1911`, `client.rs:1495-1579` | App-level WS keepalives, 45s cycle (`KEEP_ALIVE_TIMEOUT_MS=45_000`) | Sends keepalives; not ZMQ heartbeats |
| `connmgr/connection.rs:4287` | Idle-connection expiry (`expire(now)`, closes idle conns) | App-level eviction already exists |
| `core/zmq.rs:720-750` | WZMQ_* FFI constants | Only LINGER/SNDHWM/RCVHWM/TCP_KEEPALIVE*/ROUTER_MANDATORY/PROBE_ROUTER |
| `core/zmqsocket.cpp:508-533` | C++ setters | HWM/identity/immediate/router-mandatory/probe-router/TCP-keepalive only |
| Tests (`server.rs:2556`, `zhttpsocket.rs:3099`) | XPUB sockets | Test harness only (`inproc`), assert `\x01test` envelope |

Greps returning zero hits across `src/`: `set_heartbeat`,
`HEARTBEAT_`, `handover|HANDOVER`, `maxmsgsize|MAXMSGSIZE`,
`set_reconnect|reconnect_ivl`, `set_xpub_verbose|VERBOSE`.

## PR 1 (recommended): ZMQ heartbeat on connmgr sockets

**What:** set `HEARTBEAT_IVL=1000`, `HEARTBEAT_TIMEOUT=5000`,
`HEARTBEAT_TTL=5000` on the PUB fan-out (`zhttpsocket.rs:2064`) and the SUB
sockets (`client.rs:2199`, `zhttpsocket.rs:3427`). Rust-side via rust-zmq
(`set_heartbeat_ivl/timeout/ttl`), ~6 lines, no FFI change.

**Why:** no `HEARTBEAT_*` setsockopt exists anywhere in pushpin `src/`
(verified by grep). libzmq heartbeat defaults are off, so a downstream that
stops responding without closing TCP (frozen container, wedged handler) is
never reaped at the engine level: its queued HWM messages sit in memory and
the peer slot never frees. Pushpin's 45s app keepalives (`keep_alives_task`)
ride *above* ZMQ and cannot detect a peer whose TCP ACKs but whose ZMQ
engine never answers. Our proof in sqtseries: identical symptom (SIGKILL'd
subscriber counted forever), fixed by the same 3 lines, verified by
SIGSTOP-freeze eviction at 6.0s.

**Repro for the PR description:** SUB to connmgr PUB, `SIGSTOP` the
subscriber, publish for 60s, observe the dead peer never drops (before) vs
drops after ~6s idle (after).

**Scope note:** default transports in `src/internal.conf` are all `ipc://`,
where dead-peer reaping matters less; this PR targets TCP deployments
(`*_specs` pointed at `tcp://`), which is also where the 1m-connection story
lives.

## PR 2: `RECONNECT_IVL_MAX` on SUB sockets

**What:** `set_reconnect_ivl_max(5000)` next to the SUB creations above
(2 lines). libzmq default is fixed 100ms retries (`options.cpp:188-189`):
after a connmgr restart every handler SUB reconnects in lockstep and hammers
it. Same change as sqtseries `client.py` SUB socket.

## PR 3: `ROUTER_HANDOVER` on ROUTER sockets

**What:** `set_router_handover(true)` on `req_sock` / `in_stream_sock`
(`zhttpsocket.rs:2059,2063`, `client.rs:2193`, `zhttpsocket.rs:3421`).
No handover set anywhere in pushpin (grep). Prevents stale identity frames
from a previous connection being routed to the new one after a handler
reconnects (silent message loss). Analog: sqtseries `broker.py`
`router_handover=1` (`RESEARCHES.md` item 8).

## PR 4 (FFI): expose the missing socket options to C++

**What:** add `WZMQ_HEARTBEAT_IVL/TIMEOUT/TTL`, `WZMQ_XPUB_VERBOSE`,
`WZMQ_MAXMSGSIZE`, `WZMQ_RECONNECT_IVL_MAX`, `WZMQ_ROUTER_HANDOVER` to
`core/zmq.rs:734-750` plus matching `ZmqSocket::set*` in
`core/zmqsocket.cpp:508-533`. **Why:** the C++ handler/proxy side cannot set
any of these today; PRs 1-3 cover the Rust side only.

## PR 5 (hardening): `MAXMSGSIZE` cap on ingress sockets

**What:** cap inbound frames (e.g. 50MB, our value) on PULL/ROUTER ingress.
No `maxmsgsize` anywhere in pushpin (grep); ZMQ default is unlimited, so one
oversized publish can OOM a subscriber. Note: 1.41.0 already capped the
HTTP side (`push_in_http_max_{headers,body}_size`); the ZMQ ingress side is
still uncapped. Repro sketch: send a 500MB frame, watch subscriber RSS
before/after.

## PR 6 (recommended): `publish_zmq` per-call context + unbounded linger

**What:** `src/publish/mod.rs:436-445` builds a fresh `zmq::Context` per
publish, connects a PUSH socket with no `LINGER`/`SNDHWM`/reconnect settings,
blocking-sends, and drops it:

```rust
let context = zmq::Context::new();
let sock = context.socket(zmq::PUSH)?;
sock.connect(spec)?;
sock.send(message, 0)?;
```

Each `Context::new()` spawns its own I/O thread; the dropped socket keeps
default LINGER (-1, wait forever), so a wedged peer can stall the publisher
indefinitely. Proposed: share one process-wide context and set
`LINGER`/`SNDHWM`/`RECONNECT_IVL_MAX` on the PUSH socket. Compare sqtseries
`client.py`: `WRITE_LINGER_MS=2000` (verified: write-then-close with 0
delivered 0 of 1 messages), HWM set, reconnect backoff.

**Repro:** publish in a loop against a slow peer; watch thread count and
tail latency before/after. (`track.rs` checked: it is a drop-flag utility,
not subscription tracking — no PR there.)

## Checked, deliberately no PR

| sqtseries pattern | Pushpin status |
|---|---|
| `XPUB_VERBOSER` every join/leave | Not applicable: fan-out uses plain PUB (`zhttpsocket.rs:2064`) with explicit subscription forwarding; envelope already asserted in tests (`\x01test`) |
| `sweep_stale` idle sweep | Already covered: `connection.rs:4287` idle expiry + 45s `keep_alives_task` |
| XPUB welcome message | Same incompatibility we hit: single-frame welcome breaks multipart SUB clients |
| `Context.set(MAX_SOCKETS)` trap | pyzmq-specific; pushpin uses rust-zmq directly |
| `free_ports` test race | Python-test-only; not applicable |

## Proof status (honest)

- Ran: sqtseries scale proofs (3000 joins/leaves, 500 same-topic, 300 WS),
  SIGKILL/SIGSTOP eviction, `test_cache_and_health` 29/29, full suite 53/53,
  examples runner 14/14, `make style`/`lint` clean.
- Not run: pushpin Rust/C++ build (heavyweight). Each PR above carries
  file:line anchors plus a repro so maintainer CI can verify.

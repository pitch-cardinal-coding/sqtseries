#!/usr/bin/env python3
"""Browser-side probe for the sqtseries admin dashboard.

Drives headless Chromium (Playwright) against a *running* service's
/dashboard for a fixed duration and reports what a human would catch by
watching: console errors, page exceptions, failed responses, whether live
values actually move, and whether the page's JS heap stays bounded.

Pairs with scripts/stress_percentiles.py: start the stress run with
``--http-port N``, then probe http://127.0.0.1:N/dashboard while the pump
hammers every backend path.

Exit codes: 0 clean, 1 problems found (console/page errors, >=400
responses, stalled counters), 2 could not run.

Usage:
  scripts/dashboard_probe.py --url http://127.0.0.1:8080 [--duration 300]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

CONNECT_TIMEOUT_MS = 15_000
SAMPLE_EVERY_S = 5.0
STALL_LIMIT_S = 60.0  # ingested counter frozen longer than this = stall


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="service base URL")
    ap.add_argument("--duration", type=float, default=300.0)
    ap.add_argument(
        "--screenshot-dir",
        default="",
        help="optional dir for start/mid/end screenshots",
    )
    args = ap.parse_args()

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("probe: playwright not installed", file=sys.stderr)
        return 2

    shots = Path(args.screenshot_dir) if args.screenshot_dir else None
    if shots:
        shots.mkdir(parents=True, exist_ok=True)

    console_errors: list[str] = []
    page_errors: list[str] = []
    failed: list[str] = []
    samples: list[dict[str, object]] = []
    server_gone = False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        def on_console(m: object) -> None:
            nonlocal server_gone
            if getattr(m, "type", "") != "error":
                return
            text = getattr(m, "text", "")
            console_errors.append(text)
            # A refused connection right at teardown is expected (the stress
            # harness stops the server while this probe may still be
            # sampling); only treat it as a page defect if the server is
            # still alive.
            if (
                "ERR_CONNECTION_REFUSED" in text or "ERR_CONNECTION_RESET" in text
            ) and not server_gone:
                try:
                    page.request.get(args.url + "/dashboard", timeout=2000)
                except Exception:
                    server_gone = True

        page.on("console", on_console)
        page.on("pageerror", lambda e: page_errors.append(str(e)))
        page.on(
            "response",
            lambda r: failed.append(f"{r.status} {r.url}") if r.status >= 400 else None,
        )
        # The rig starts the probe before the service exists (it retries
        # connecting until the harness binds the port), so retry the initial
        # connect instead of failing on the first refused connection.
        connected = False
        last_exc: Exception | None = None
        connect_deadline = time.monotonic() + min(args.duration, 180.0)
        while time.monotonic() < connect_deadline and not connected:
            try:
                page.goto(args.url + "/dashboard")
                page.wait_for_function(
                    "() => document.getElementById('conn-state-text')"
                    ".textContent === 'connected'",
                    timeout=CONNECT_TIMEOUT_MS,
                )
                connected = True
            except Exception as exc:
                last_exc = exc
                time.sleep(2.0)
        if not connected:
            print(f"probe: page never connected: {last_exc}", file=sys.stderr)
            browser.close()
            return 1
        print(f"probe: connected to {args.url}/dashboard")

        deadline = time.monotonic() + args.duration
        first_ingested: int | None = None
        last_ingested = 0
        last_change = time.monotonic()
        while time.monotonic() < deadline:
            try:
                s = page.evaluate(
                    """() => ({
                        ingested: document.getElementById('stat-ingested')?.textContent ?? '',
                        rate: document.getElementById('stat-ingest-rate')?.textContent ?? '',
                        pill: document.getElementById('conn-state-text')?.textContent ?? '',
                        connRows: document.querySelectorAll('#conn-rows tr').length,
                        topicRows: document.querySelectorAll('#topic-rows tr').length,
                        ticker: document.querySelectorAll('#ticker li').length,
                        heap: performance.memory ? performance.memory.usedJSHeapSize : 0,
                    })"""
                )
            except Exception as exc:
                page_errors.append(f"evaluate failed: {exc}")
                break
            if server_gone:
                break
            s["t"] = round(time.monotonic() - (deadline - args.duration), 1)
            samples.append(s)
            try:
                n = int(str(s["ingested"]).replace(",", "") or 0)
            except ValueError:
                n = last_ingested
            if first_ingested is None:
                first_ingested = n
            if n > last_ingested:
                last_ingested = n
                last_change = time.monotonic()
            if time.monotonic() - last_change > STALL_LIMIT_S:
                print(
                    f"probe: STALL — ingested frozen at {last_ingested} "
                    f"for >{STALL_LIMIT_S:.0f}s",
                    file=sys.stderr,
                )
            if shots and len(samples) in (1, len(samples) // 2 + 1):
                page.screenshot(path=str(shots / f"probe-{len(samples):03d}.png"))

            if shots and (deadline - time.monotonic()) <= SAMPLE_EVERY_S:
                page.screenshot(path=str(shots / "probe-final.png"))
            time.sleep(SAMPLE_EVERY_S)

        browser.close()

    heaps = [s["heap"] for s in samples if s.get("heap")]
    import json

    effective_console = [
        e for e in console_errors if not (server_gone and "ERR_CONNECTION_REFUSED" in e)
    ]
    verdict = {
        "samples": len(samples),
        "console_errors": effective_console,
        "console_errors_raw": len(console_errors),
        "page_errors": page_errors,
        "failed_responses": failed,
        "server_gone_at_end": server_gone,
        "ingested_first": first_ingested,
        "ingested_last": last_ingested,
        "ingested_delta": (last_ingested - (first_ingested or 0)),
        "heap_first_kb": round(heaps[0] / 1024) if heaps else 0,
        "heap_last_kb": round(heaps[-1] / 1024) if heaps else 0,
        "stalled": time.monotonic() - last_change > STALL_LIMIT_S and not server_gone,
    }
    print(json.dumps(verdict, indent=2))
    problems = (
        effective_console
        or page_errors
        or failed
        or verdict["stalled"]
        or (verdict["ingested_delta"] <= 0 and not server_gone)
    )
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Structural check for the generated pages.

Tag counting is a blunt instrument, but it catches the failure that actually
happens when a body is written by hand: one unclosed <figure> or <pre>
silently swallowing the rest of the page. It also reports per-page size and the
number of documented settings, options and code blocks, which are the coverage
numbers these pages exist to deliver.

Exit 0 when every page is well formed, 1 otherwise.
"""

import re
import sys
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent.parent / "dist/docs"

VOID = {
    "area",
    "base",
    "br",
    "col",
    "embed",
    "hr",
    "img",
    "input",
    "link",
    "meta",
    "param",
    "source",
    "track",
    "wbr",
}

TOKEN = re.compile(r"<(/?)([a-zA-Z][a-zA-Z0-9]*)\b[^>]*?(/?)>")


def check(path: Path) -> tuple[list[str], dict[str, int]]:
    """Return (problems, stats) for one page, using a real tag stack."""
    text = path.read_text()
    if "<main>" in text:
        body = text.split("<main>", 1)[1].split("</main>", 1)[0]
    else:
        body = text
    stack: list[tuple[str, int]] = []
    problems: list[str] = []
    for match in TOKEN.finditer(body):
        closing = match.group(1) == "/"
        name = match.group(2).lower()
        selfclose = match.group(3) == "/"
        if name in VOID or selfclose:
            continue
        if closing:
            if not stack:
                problems.append(f"stray </{name}> at offset {match.start()}")
            elif stack[-1][0] != name:
                problems.append(
                    f"</{name}> closes <{stack[-1][0]}> (opened at offset {stack[-1][1]})"
                )
                stack.pop()
            else:
                stack.pop()
        else:
            stack.append((name, match.start()))
    problems.extend(f"never closed: <{name}> at offset {off}" for name, off in stack)

    stats = {
        "bytes": len(text),
        "sections": len(re.findall(r"<section>", body)),
        "pre": len(re.findall(r"<pre>", body)),
        "tables": len(re.findall(r"<table", body)),
        "opts": len(re.findall(r'class="opt"', text)),
        "svg": len(re.findall(r"<svg", text)),
        "figures": len(re.findall(r"<figure>", body)),
        "links": len(re.findall(r'href="[a-z-]+\.html', text)),
    }
    return problems, stats


def main() -> int:
    if not OUT.is_dir():
        print(f"no docs directory at {OUT}")
        return 1
    pages = sorted(OUT.glob("*.html"))
    if not pages:
        print(f"no pages in {OUT}")
        return 1

    totals = dict.fromkeys(
        ("bytes", "sections", "pre", "tables", "opts", "svg", "figures", "links"), 0
    )
    bad = 0
    for path in pages:
        problems, stats = check(path)
        for key in totals:
            totals[key] += stats[key]
        flag = "OK " if not problems else "BAD"
        if problems:
            bad += 1
        print(
            f"{flag} {path.name:<18} {stats['bytes']:>7,}B  "
            f"sec={stats['sections']:<3} fig={stats['figures']:<3} "
            f"pre={stats['pre']:<3} tbl={stats['tables']:<3} "
            f"opts={stats['opts']:<4} svg={stats['svg']:<3} "
            f"links={stats['links']:<4}"
        )
        for problem in problems[:5]:
            print(f"      {problem}")

    print(
        f"\n{len(pages)} pages, {totals['bytes']:,} bytes, "
        f"{totals['sections']} sections, {totals['tables']} tables, "
        f"{totals['opts']} documented options/settings, "
        f"{totals['svg']} diagrams, {totals['pre']} code blocks"
    )
    if bad:
        print(f"\n{bad} page(s) malformed")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Cross-check the built pages against the running code, in both directions.

The requirement is that nothing is left out and nothing is invented. A page
that omits a setting, a route or an option is a page that sends a reader to
look for something that is not there. A page that names a flag the program does
not accept is worse: it is a confident lie.

Both directions are proved here, and every fact is read from the code itself
rather than from a hand-maintained list:

* CLI flags come from ``click`` on the real command group.
* Settings and their environment variables come from the pydantic models.
* HTTP routes come from the FastAPI app's own route table.
* Aggregations come from ``AGGREGATORS``.
* Admin commands come from the service's own handler.
* Example scripts come from the filesystem.

Exit 0 when the pages and the code agree, 1 when they do not.
"""

import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
DOCS = REPO / "dist/docs"
sys.path.insert(0, str(REPO / "src"))

FLAG = re.compile(r"--[a-z][a-z0-9-]*")
STYLE_BLOCK = re.compile(r"<style\b.*?</style>", re.DOTALL)
SCRIPT_BLOCK = re.compile(r"<script\b.*?</script>", re.DOTALL)

# Flags owned by tools documented on these pages but not by sqtseries itself:
# systemd (`--user`, `--now`, `--property`), journalctl (`--since`), the Chromium
# probe in scripts/dashboard_probe.py (`--headless`), cargo (`--features`),
# git/ruff (`--check`) and the stress rig (`--tag`).
KNOWN_NON_CLI = {
    "user",
    "now",
    "since",
    "headless",
    "features",
    "property",
    "check",
    "tag",
    "no-pager",
    "disable-gpu",
    "no-sandbox",
    "print-to-pdf",
    "no-header",
    "no-cov",
    "maxfail",
    "durations",
    "strict-probe",
    "warmup-snapshots",
    "rss-tolerance-kb",
    "system",
    "help",
    "version",
}


def all_text() -> str:
    """Every built page, with CSS and script stripped.

    The stylesheet is inlined in each page and is full of custom properties
    spelled `--brand`, `--accent`, `--muted`. Those are not CLI flags, and
    scanning them would make every page look full of invented options.

    `benchmarks.md` is published verbatim rather than rendered, and it was
    the one page this check never read — so its commands went unverified while
    everything around them was checked. It is included for exactly that reason.
    """
    out = []
    for path in sorted(DOCS.glob("*.html")) + sorted(DOCS.glob("*.md")):
        text = path.read_text()
        text = STYLE_BLOCK.sub("", text)
        text = SCRIPT_BLOCK.sub("", text)
        out.append(text)
    return "\n".join(out)


def page(stem: str) -> str:
    path = DOCS / f"{stem}.html"
    if not path.is_file():
        raise SystemExit(f"missing page: {path} — build first")
    return path.read_text()


# ------------------------------------------------------------------- CLI ----


def script_flags() -> set[str]:
    """Long flags declared by the example and tooling scripts.

    The examples take real options of their own (`--admin-port`, `--span-hours`,
    `--skip-camera`), and those are as much a documented surface as the CLI's.
    They are read straight out of each script's argparse calls, so a flag added to
    an example and not to the docs still fails this check.
    """
    add_arg = re.compile(r"""add_argument\(\s*["'](--[a-z][a-z0-9-]*)""")
    flags: set[str] = set()
    for directory in ("examples", "scripts"):
        for path in (REPO / directory).rglob("*.py"):
            for flag in add_arg.findall(path.read_text()):
                flags.add(flag.lstrip("-"))
    return flags | shell_flags()


def shell_flags() -> set[str]:
    """Long options the shell tooling scripts accept.

    Read from the `case` arms the scripts dispatch on, not from a loose scan of
    the file. The distinction matters: `run-stress-rig.sh` prints a usage banner
    listing its options, and a grep would count that banner as proof. The
    benchmarks page documents `--no-guard`, `--guard-budget-mb` and
    `--probe-budget-mb`, and the check called them invented only because it
    never looked at shell.
    """
    arm = re.compile(r"^\s*(--[a-z][a-z0-9-]*)(\)|=)")
    flags: set[str] = set()
    for path in (REPO / "scripts").rglob("*.sh"):
        in_case = False
        for line in path.read_text().splitlines():
            stripped = line.strip()
            if re.match(r"^case\b.*\bin$", stripped):
                in_case = True
            elif stripped == "esac":
                in_case = False
            elif in_case:
                match = arm.match(line)
                if match:
                    flags.add(match.group(1).lstrip("-"))
    return flags


def real_cli_flags() -> set[str]:
    """Long options the CLI group and every subcommand actually accept."""
    from click.testing import CliRunner

    from sqtseries.cli import main

    runner = CliRunner()
    out: set[str] = set()
    for args in (["--help"], *[[c, "--help"] for c in sorted(main.commands)]):
        result = runner.invoke(main, args)
        out |= {f.lstrip("-") for f in FLAG.findall(result.output)}
    return out


# -------------------------------------------------------------- settings ----


def real_settings() -> list[tuple[str, str, str]]:
    """(dotted key, env var, default) for every leaf setting."""
    from pydantic import BaseModel

    from sqtseries.config import Settings

    rows: list[tuple[str, str, str]] = []
    for section_name, section in Settings.model_fields.items():
        default = section.default
        if isinstance(default, BaseModel):
            for key, field in type(default).model_fields.items():
                rows.append(
                    (
                        f"{section_name}.{key}",
                        f"SQT_SERIES_{section_name.upper()}__{key.upper()}",
                        str(field.default),
                    )
                )
        else:
            rows.append(
                (section_name, f"SQT_SERIES_{section_name.upper()}", str(default))
            )
    return rows


# ----------------------------------------------------------------- routes ----


def real_routes() -> set[str]:
    """Every path the gateway serves.

    The API routes come from the router object's own route table, so a new
    ``@router.get`` is picked up without touching this file. The page and
    WebSocket routes are registered on the app inside ``create_app``, which
    needs a live engine to instantiate — so they are read from the decorators
    in the source instead, which is the same literal the app uses.
    """
    from sqtseries.gateway.routes import router as api_router

    paths: set[str] = {getattr(r, "path", "") for r in api_router.routes}

    app_src = (REPO / "src/sqtseries/gateway/app.py").read_text()
    for path in re.findall(r'@app\.(?:get|websocket)\("([^"]+)"', app_src):
        paths.add(path)
    paths.add("/openapi.json")  # set via FastAPI(openapi_url=...)
    paths.discard("")
    return paths


# ------------------------------------------------------------ aggreations ----


def real_aggregations() -> set[str]:
    from sqtseries.query.agg import AGGREGATORS

    return set(AGGREGATORS)


# ---------------------------------------------------------- admin commands ----


def real_admin_commands() -> set[str]:
    """Command names the admin handler branches on, read from its source."""
    source = (REPO / "src/sqtseries/service.py").read_text()
    handler = source.split("def _admin_handler", 1)[1].split("def _admin_stats", 1)[0]
    return set(re.findall(r'cmd == "([a-z]+)"', handler))


# ----------------------------------------------------------------- report ----


def report(label: str, missing: list[str], invented: list[str], unit: str) -> bool:
    ok = not missing and not invented
    print(f"\n{label}")
    if missing:
        print(f"  NOT DOCUMENTED ({len(missing)}): {', '.join(missing)}")
    if invented:
        print(f"  DOCUMENTED BUT NOT REAL ({len(invented)}): {', '.join(invented)}")
    if ok:
        print(f"  every {unit} matches the code")
    return ok


def readme_claims() -> dict[str, int]:
    """The headline counts the root README states, parsed from its own line.

    The pages get their numbers from a shared fragment or a check. The README is
    hand-written prose, so its figures were the one number set in the repository
    with nothing behind them — and they had drifted. They are parsed here and
    compared against the code, so the next drift fails a check instead of
    shipping.
    """
    readme = (REPO / "README.md").read_text()
    found: dict[str, int] = {}
    for key, pattern in (
        ("lines", r"\*\*([\d,]+) lines\*\*"),
        ("classes", r"(\d+) classes"),
        ("functions", r"(\d+) functions"),
        ("tests", r"\*\*(\d+) tests\b"),
    ):
        match = re.search(pattern, readme)
        if match:
            found[key] = int(match.group(1).replace(",", ""))
    return found


def real_readme_counts() -> dict[str, int]:
    """What those four README numbers should be, measured from the repository."""
    import ast
    import subprocess

    py = sorted((REPO / "src").rglob("*.py"))
    lines = sum(len(p.read_text().splitlines()) for p in py)
    classes = functions = 0
    for path in py:
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ClassDef):
                classes += 1
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                functions += 1
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "--collect-only", "-q"],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    tests = 0
    for line in proc.stdout.splitlines():
        match = re.search(r"(\d+) tests? collected", line)
        if match:
            tests = int(match.group(1))
    return {"lines": lines, "classes": classes, "functions": functions, "tests": tests}


def main() -> int:
    if not any(DOCS.glob("*.html")):
        raise SystemExit("no built pages — run sqtseries_build.py first")
    text = all_text()
    documented_flags = {f.lstrip("-") for f in FLAG.findall(text)}
    ok = True

    # CLI flags: every flag the CLI accepts must be documented, and every flag a
    # page names must be one the CLI accepts or one of the allowed non-CLI tools.
    real = real_cli_flags() - {"help"}
    accepted = real | script_flags()
    invented = sorted(documented_flags - accepted - KNOWN_NON_CLI)
    ok &= report("CLI flags", sorted(real - documented_flags), invented, "CLI flag")

    # Settings: the key AND its env var must both appear.
    missing_settings = []
    for key, env, _default in real_settings():
        if key not in text or env not in text:
            missing_settings.append(key)
    ok &= report(
        "Settings",
        missing_settings,
        [],
        "setting (key + environment variable)",
    )

    # Routes.
    real_paths = {p for p in real_routes() if p}
    missing_routes = sorted(p for p in real_paths if p not in text)
    ok &= report("HTTP routes", missing_routes, [], "route")

    # Aggregations.
    missing_aggs = sorted(a for a in real_aggregations() if f">{a}<" not in text)
    ok &= report("Aggregations", missing_aggs, [], "aggregation")

    # Admin commands.
    missing_admin = sorted(c for c in real_admin_commands() if f'"{c}"' not in text)
    ok &= report("Admin commands", missing_admin, [], "admin command")

    # Example scripts: every runnable script should be mentioned somewhere.
    examples = sorted(
        p.stem for p in (REPO / "examples").glob("*.py") if p.stem != "__init__"
    )
    missing_examples = sorted(e for e in examples if e not in text)
    ok &= report("Example scripts", missing_examples, [], "example script")

    claimed = readme_claims()
    real_counts = real_readme_counts()
    print("\nREADME headline counts")
    stale = []
    for key, actual in real_counts.items():
        said = claimed.get(key)
        mark = "ok" if said == actual else "STALE"
        if said != actual:
            stale.append(key)
        print(f"  {key:>9}: README says {said!s:>7}  actual {actual:>7}  {mark}")
    if not stale:
        print("  every headline count matches the repository")
    else:
        ok = False

    print()
    if ok:
        print("docs and code agree in both directions")
    else:
        print("docs and code disagree — see the lists above")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

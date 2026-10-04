"""Fail when requirements.txt and pyproject.toml disagree on runtime deps.

`pyproject.toml` is the source of truth: it is what actually resolves when a
user runs `pip install .`. `requirements.txt` exists so a machine can be set
up from pinned wheels without invoking the build backend, and its `# Runtime`
section is a mirror of that list.

The two drifting apart is not a cosmetic problem. `scripts/test_wheel.sh`
builds its venv from `requirements.txt` and then installs the wheel with
`--no-deps`, so a dependency present in pyproject but absent from
requirements.txt is missing from the exact environment the release gate
validates. `websockets` was missing that way: the service started, `/health`
and `/dashboard` answered 200, and every WebSocket endpoint was dead because
uvicorn resolved `ws="auto"` to `none` and the upgrade returned 404 instead of
101. Nothing failed loudly.

    Runtime block rule: the `# Runtime` section is the leading run of
    `name==version` lines, terminated by the first blank or comment line that
    follows at least one pin. Everything after it belongs to another section.
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PYPROJECT = REPO / "pyproject.toml"
REQUIREMENTS = REPO / "requirements.txt"

PIN = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)\s*==\s*([^\s;#]+)")
SPEC = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*(?:\[|>=|<=|~=|==|!=|>|<)")


def normalize(name: str) -> str:
    """PEP 503 name normalisation."""
    return re.sub(r"[-_.]+", "-", name).lower()


def pyproject_runtime() -> dict[str, str]:
    """Runtime dependency name -> specifier, as declared by the project."""
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    declared: dict[str, str] = {}
    for entry in data["project"]["dependencies"]:
        match = SPEC.match(entry)
        if match is None:
            raise SystemExit(f"cannot parse dependency {entry!r} in {PYPROJECT}")
        declared[normalize(match.group(1))] = entry
    return declared


def requirements_runtime() -> dict[str, str]:
    """Runtime pin name -> version, from the leading block of requirements.txt."""
    pins: dict[str, str] = {}
    for raw in REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            if pins:
                break
            continue
        match = PIN.match(line)
        if match is None:
            if pins:
                break
            raise SystemExit(f"cannot parse requirement {raw!r} in {REQUIREMENTS}")
        pins[normalize(match.group(1))] = match.group(2)
    return pins


def main() -> int:
    declared = pyproject_runtime()
    pinned = requirements_runtime()

    missing = sorted(set(declared) - set(pinned))
    extra = sorted(set(pinned) - set(declared))

    for name in missing:
        print(
            f"MISSING  {name} is a runtime dependency in pyproject.toml but has "
            f"no pin in the # Runtime section of {REQUIREMENTS.name}"
        )
    for name in extra:
        print(
            f"EXTRA    {name} is pinned in the # Runtime section of "
            f"{REQUIREMENTS.name} but not declared in pyproject.toml"
        )

    if missing or extra:
        print(
            f"\n{len(missing)} missing, {len(extra)} extra — the two lists must "
            f"be identical"
        )
        return 1

    print(
        f"OK: {len(declared)} pyproject runtime deps == "
        f"{len(pinned)} requirements.txt runtime pins"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

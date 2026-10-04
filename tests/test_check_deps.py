"""Tests for scripts/check_deps.py, the requirements.txt <-> pyproject gate.

Each test pins one behaviour of the drift check. The regression they exist to
prevent is concrete: `websockets` was declared in pyproject.toml but absent from
requirements.txt, so scripts/test_wheel.sh built a venv without it and the
wheel it then validated had dead WebSocket endpoints.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SCRIPT = REPO / "scripts" / "check_deps.py"


def _load(tmp_path, pyproject_body: str, requirements_body: str):
    """Import check_deps with PYPROJECT/REQUIREMENTS pointed at temp files."""
    root = tmp_path / "repo"
    (root / "scripts").mkdir(parents=True)
    target = root / "scripts" / "check_deps.py"
    target.write_text(SCRIPT.read_text(encoding="utf-8"), encoding="utf-8")
    (root / "pyproject.toml").write_text(pyproject_body, encoding="utf-8")
    (root / "requirements.txt").write_text(requirements_body, encoding="utf-8")

    spec = importlib.util.spec_from_file_location(f"check_deps_{tmp_path.name}", target)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


PYPROJECT = """
[project]
name = "sqtseries"
dependencies = [
    "click>=8.1",
    "websockets>=17.0",
]

[project.optional-dependencies]
dev = ["pytest>=8"]
"""


def test_repo_requirements_match_pyproject():
    """The real files agree — the gate that protects every wheel build."""
    spec = importlib.util.spec_from_file_location("check_deps_repo", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    assert module.main() == 0, (
        "requirements.txt and pyproject.toml disagree on runtime deps; "
        "run scripts/check_deps.py for the detail"
    )


def test_websockets_is_pinned(tmp_path):
    """The specific regression: websockets was declared but never pinned."""
    reqs = "# Runtime\nclick==8.4.2\nwebsockets==17.0.1\n\n# Testing\npytest==9.1.1\n"
    module = _load(tmp_path, PYPROJECT, reqs)

    assert set(module.requirements_runtime()) == {"click", "websockets"}


def test_missing_pin_is_reported(tmp_path, capsys):
    reqs = "# Runtime\nclick==8.4.2\n\n# Testing\npytest==9.1.1\n"
    module = _load(tmp_path, PYPROJECT, reqs)

    assert module.main() == 1
    out = capsys.readouterr().out
    assert "MISSING" in out
    assert "websockets" in out


def test_extra_pin_is_reported(tmp_path, capsys):
    reqs = "# Runtime\nclick==8.4.2\nwebsockets==17.0.1\norjson==3.11.9\n"
    module = _load(tmp_path, PYPROJECT, reqs)

    assert module.main() == 1
    out = capsys.readouterr().out
    assert "EXTRA" in out
    assert "orjson" in out


def test_matching_sets_pass(tmp_path, capsys):
    reqs = "# Runtime\nclick==8.4.2\nwebsockets==17.0.1\n\n# Testing\npytest==9.1.1\n"
    module = _load(tmp_path, PYPROJECT, reqs)

    assert module.main() == 0
    assert "OK" in capsys.readouterr().out


@pytest.mark.parametrize(
    "spelling", ["typing_extensions", "typing-extensions", "Typing.Extensions"]
)
def test_pep503_normalisation(tmp_path, spelling):
    """Separators are equivalent to a resolver, and case is not significant."""
    module = _load(tmp_path, PYPROJECT, "# Runtime\nclick==8.4.2\n")
    assert module.normalize(spelling) == "typing-extensions"


def test_a_different_spelling_is_a_different_package(tmp_path):
    """Normalising collapses separators; it does not delete them."""
    module = _load(tmp_path, PYPROJECT, "# Runtime\nclick==8.4.2\n")
    assert module.normalize("web_sockets") != module.normalize("websockets")


def test_runtime_block_stops_at_the_next_section(tmp_path):
    """A pin under `# Testing` is not a runtime pin, even when it is first."""
    reqs = (
        "# Runtime\nclick==8.4.2\nwebsockets==17.0.1\n"
        "\n# Testing\npytest==9.1.1\nhypothesis==6.165.2\n"
    )
    module = _load(tmp_path, PYPROJECT, reqs)

    assert set(module.requirements_runtime()) == {"click", "websockets"}


def test_version_bounds_do_not_affect_membership(tmp_path):
    """`websockets>=17.0` and `websockets==17.0.1` are the same package."""
    reqs = "# Runtime\nclick==8.4.2\nwebsockets==17.0.1\n"
    module = _load(tmp_path, PYPROJECT, reqs)

    declared = module.pyproject_runtime()
    pinned = module.requirements_runtime()

    assert declared["websockets"] == "websockets>=17.0"
    assert pinned["websockets"] == "17.0.1"
    assert set(declared) == set(pinned)

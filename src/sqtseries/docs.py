"""Locate the documentation site shipped with the package."""

from pathlib import Path


def find_docs_dir() -> Path | None:
    """Return the directory holding index.html, or None when absent.

    Release wheels carry a docs_data copy (see scripts/build_wheel.sh). A source
    checkout falls back to dist/docs next to the repo root. Callers must treat
    None as "no documentation available" rather than linking to it anyway.
    """
    packaged = Path(__file__).resolve().parent / "docs_data"
    if (packaged / "index.html").is_file():
        return packaged
    repo = Path(__file__).resolve().parent.parent.parent / "dist" / "docs"
    if (repo / "index.html").is_file():
        return repo
    return None


DOCS_DIR = find_docs_dir()

"""In-tree PEP 517 backend: generate the doc pages, then hand off to setuptools.

setuptools has no hook for "run a generator before packaging", and the doc
pages are served by the gateway at /docs straight out of the installed
package. They therefore have to exist by the time the wheel is assembled.

`scripts/build_wheel.sh` used to copy them in by hand, which meant only the
wheel it produced had documentation: `pip install .` — the first command in
the README — installed a service whose /docs returned 404. Generating here
makes every install path carry the docs, because every install path goes
through this backend.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

from setuptools import build_meta as _setuptools

REPO = Path(__file__).resolve().parent.parent
GENERATOR = REPO / "scripts/docs-build/sqtseries_build.py"
BUILT_DOCS = REPO / "dist/docs"
TARGET = REPO / "src/sqtseries/docs_data"


def _generate_docs() -> None:
    """Publish dist/docs into the package as sqtseries/docs_data."""
    if not GENERATOR.is_file():
        # An sdist built without the generator still ships whatever
        # docs_data it was packaged with, so there is nothing to add.
        return
    subprocess.run(  # noqa: S603
        [sys.executable, str(GENERATOR)], cwd=REPO, check=True
    )
    if not (BUILT_DOCS / "index.html").is_file():
        raise RuntimeError(f"docs build produced no {BUILT_DOCS / 'index.html'}")
    shutil.rmtree(TARGET, ignore_errors=True)
    shutil.copytree(BUILT_DOCS, TARGET)


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    _generate_docs()
    return _setuptools.build_wheel(wheel_directory, config_settings, metadata_directory)


def build_sdist(sdist_directory, config_settings=None):
    _generate_docs()
    return _setuptools.build_sdist(sdist_directory, config_settings)


def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    _generate_docs()
    return _setuptools.build_editable(
        wheel_directory, config_settings, metadata_directory
    )

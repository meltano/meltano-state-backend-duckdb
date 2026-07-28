from __future__ import annotations

import shutil
from importlib.metadata import version
from typing import TYPE_CHECKING

import pytest
from meltano.core.project import Project

if TYPE_CHECKING:
    from pathlib import Path


def pytest_report_header() -> list[str]:
    """Add Meltano version to test report header."""
    return [f"Meltano v{version('meltano')}"]


@pytest.fixture
def project(tmp_path: Path) -> Project:
    path = tmp_path / "project"
    shutil.copytree(
        "fixtures/explicit",
        path,
        ignore=shutil.ignore_patterns(".meltano"),
    )
    return Project(path.resolve())


@pytest.fixture
def project_with_uri(tmp_path: Path) -> Project:
    path = tmp_path / "project"
    shutil.copytree(
        "fixtures/only_uri",
        path,
        ignore=shutil.ignore_patterns(".meltano"),
    )
    return Project(path.resolve())

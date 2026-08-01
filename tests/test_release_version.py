from __future__ import annotations

from pathlib import Path
import tomllib

from click.testing import CliRunner

from actionq import __version__
from actionq.cli import cli


def test_source_metadata_and_cli_version_are_identical():
    metadata = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
    assert __version__ == metadata["project"]["version"]
    result = CliRunner().invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_container_installs_the_vendored_contract_package_before_actionq():
    dockerfile = (Path(__file__).parents[1] / "Dockerfile").read_text()
    assert "pip3 install /app/packages/actionq-contracts /app" in dockerfile

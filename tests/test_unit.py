import json

import pytest
from click.testing import CliRunner

from actionq import db
from actionq.cli import cli


def test_schema_name_defaults(monkeypatch):
    monkeypatch.delenv("ACTIONQ_SCHEMA", raising=False)
    assert db.schema_name() == "actionq"


def test_schema_name_rejects_unsafe_identifier():
    with pytest.raises(db.ActionQError):
        db.schema_name("bad-name")


def test_parse_json_rejects_invalid_payload():
    with pytest.raises(db.ActionQError):
        db.parse_json("{", default={})


def test_cli_requires_actionq_url_for_migrate(monkeypatch):
    monkeypatch.delenv("ACTIONQ_URL", raising=False)
    result = CliRunner().invoke(cli, ["migrate"])
    assert result.exit_code != 0
    assert "ACTIONQ_URL is required" in result.output


def test_json_default_serializes_plain_values():
    payload = json.loads(db.to_json({"x": 1}))
    assert payload == {"x": 1}

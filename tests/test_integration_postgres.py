import json
import os
import uuid

import pytest
from click.testing import CliRunner

try:
    import psycopg  # noqa: F401
except ModuleNotFoundError:
    psycopg = None

from actionq.cli import cli


pytestmark = pytest.mark.skipif(
    psycopg is None or not os.environ.get("ACTIONQ_TEST_URL"),
    reason="ACTIONQ_TEST_URL and psycopg are required for Postgres integration tests",
)


@pytest.fixture
def runner_env(monkeypatch):
    schema = "aqtest_" + uuid.uuid4().hex
    monkeypatch.setenv("ACTIONQ_URL", os.environ["ACTIONQ_TEST_URL"])
    monkeypatch.setenv("ACTIONQ_SCHEMA", schema)
    return CliRunner(), schema


def _invoke_json(runner, args):
    result = runner.invoke(cli, args)
    assert result.exit_code == 0, result.output
    return json.loads(result.output)


def test_lifecycle_claim_complete_show(runner_env):
    runner, _schema = runner_env
    result = runner.invoke(cli, ["migrate"])
    assert result.exit_code == 0, result.output

    action = _invoke_json(
        runner,
        [
            "add",
            "--type",
            "scope-iterate",
            "--project",
            "sprintctl",
            "--target",
            "42",
            "--source",
            "doc:plan",
            "--created-by",
            "human:test",
        ],
    )
    assert action["status"] == "pending"

    claimed = _invoke_json(runner, ["claim", "--worker", "worker:test"])
    assert claimed["id"] == action["id"]
    assert claimed["status"] == "claimed"

    completed = _invoke_json(
        runner,
        ["complete", str(action["id"]), "--result", "branch=agent/scope-iterate/1"],
    )
    assert completed["status"] == "completed"

    detail = _invoke_json(runner, ["show", str(action["id"])])
    assert detail["action"]["status"] == "completed"
    assert [event["event_type"] for event in detail["events"]] == [
        "action_enqueued",
        "action_claimed",
        "action_completed",
    ]


def test_claim_exits_nonzero_when_empty(runner_env):
    runner, _schema = runner_env
    result = runner.invoke(cli, ["migrate"])
    assert result.exit_code == 0, result.output

    result = runner.invoke(cli, ["claim", "--worker", "worker:test"])
    assert result.exit_code == 2
    assert "no pending actions" in result.output


def test_emit_coordinator_cycle(runner_env):
    runner, _schema = runner_env
    result = runner.invoke(cli, ["migrate"])
    assert result.exit_code == 0, result.output

    event = _invoke_json(
        runner,
        [
            "emit",
            "--type",
            "coordinator_cycle",
            "--actor",
            "dispatcher:test",
            "--payload",
            '{"claimed": false}',
        ],
    )
    assert event["event_type"] == "coordinator_cycle"
    assert event["payload"]["claimed"] is False

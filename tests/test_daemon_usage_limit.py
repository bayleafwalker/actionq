from __future__ import annotations

import sys
from pathlib import Path

from actionq.daemon import ActionConfig, Daemon, DaemonConfig

from tests.test_daemon import FakeClient


def _wrapper_script(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "fake_harness.py"
    path.write_text(body, encoding="utf-8")
    return path


def _daemon(tmp_path: Path, action_config: ActionConfig, client: FakeClient) -> Daemon:
    config = DaemonConfig(
        session_state_path=tmp_path / "state.json",
        pause_file=tmp_path / "PAUSED",
        handoff_dir=tmp_path / "handoff",
    )
    return Daemon(config, {"scope-iterate": action_config}, client), config


# -- deterministic command-wrapper simulations ---------------------------


def test_confirmed_usage_limit_signal_pauses_before_failing(tmp_path: Path):
    client = FakeClient({"id": 30, "action_type": "scope-iterate"})
    script = _wrapper_script(
        tmp_path,
        "import sys\n"
        "print('Error: rate limit exceeded, please try again later')\n"
        "sys.exit(1)\n",
    )
    action_config = ActionConfig(runner="command", harness="claude", command=(sys.executable, str(script)))
    daemon, config = _daemon(tmp_path, action_config, client)

    assert daemon.run_once() is True

    event_types = [event[0] for event in client.events]
    assert "session.paused" in event_types
    paused_index = event_types.index("session.paused")
    exited_index = event_types.index("session.exited")
    assert paused_index < exited_index, "session.paused must be visible before the terminal session.exited"

    paused_payload = client.events[paused_index][3]
    assert paused_payload["reason"] == "usage-limit"
    assert paused_payload["mechanism"] == "checkpoint-and-fail"
    assert paused_payload["resumable"] is True
    assert paused_payload["handoff_ref"] is not None
    handoff_path = Path(paused_payload["handoff_ref"])
    assert handoff_path.exists()
    handoff_text = handoff_path.read_text(encoding="utf-8")
    assert "rate limit exceeded" in handoff_text
    assert "session.resumed" in handoff_text  # documents the resume/re-dispatch path

    assert client.failed and client.failed[0][0] == 30
    assert client.failed[0][1].startswith("usage-limit-paused:")
    assert not client.completed

    exited_payload = client.events[exited_index][3]
    assert exited_payload["usage_limit_paused"] is True


def test_ordinary_failure_is_not_misclassified_as_a_pause(tmp_path: Path):
    client = FakeClient({"id": 31, "action_type": "scope-iterate"})
    script = _wrapper_script(
        tmp_path,
        "import sys\nprint('Traceback: boom')\nsys.exit(1)\n",
    )
    action_config = ActionConfig(runner="command", harness="claude", command=(sys.executable, str(script)))
    daemon, config = _daemon(tmp_path, action_config, client)

    assert daemon.run_once() is True

    event_types = [event[0] for event in client.events]
    assert "session.paused" not in event_types
    assert client.failed and client.failed[0][0] == 31
    assert client.failed[0][1] == "daemon session failed"
    exited_payload = client.events[event_types.index("session.exited")][3]
    assert exited_payload["usage_limit_paused"] is False


def test_successful_command_run_is_never_classified(tmp_path: Path):
    client = FakeClient({"id": 32, "action_type": "scope-iterate"})
    script = _wrapper_script(tmp_path, "print('rate limit exceeded')\n")  # exit 0 despite the phrase
    action_config = ActionConfig(runner="command", harness="claude", command=(sys.executable, str(script)))
    daemon, config = _daemon(tmp_path, action_config, client)

    assert daemon.run_once() is True

    assert "session.paused" not in [event[0] for event in client.events]
    assert client.completed and client.completed[0][0] == 32


def test_missing_harness_classification_key_never_pauses(tmp_path: Path):
    client = FakeClient({"id": 33, "action_type": "scope-iterate"})
    script = _wrapper_script(tmp_path, "import sys\nprint('rate limit exceeded')\nsys.exit(1)\n")
    # No `harness=` set: classification is intentionally inert without a
    # named confirmed-signal set, rather than guessing one.
    action_config = ActionConfig(runner="command", command=(sys.executable, str(script)))
    daemon, config = _daemon(tmp_path, action_config, client)

    assert daemon.run_once() is True

    assert "session.paused" not in [event[0] for event in client.events]
    assert client.failed and client.failed[0][1] == "daemon session failed"


# -- manual resume/re-dispatch drill --------------------------------------


def test_manual_resume_drill_emits_correlated_session_resumed(tmp_path: Path):
    client = FakeClient()
    daemon, config = _daemon(tmp_path, ActionConfig(), client)

    daemon.emit_resume_event(
        action_id=99,
        session_id="aqs:new-session",
        resumed_from_session_id="aqs:old-session",
        handoff_ref="/home/agent/.local/state/actionq/handoff/aqs_old-session.md",
    )

    assert len(client.events) == 1
    event_type, action_id, actor, payload = client.events[0]
    assert event_type == "session.resumed"
    assert action_id == 99
    assert payload == {
        "session_id": "aqs:new-session",
        "resumed_from_session_id": "aqs:old-session",
        "handoff_ref": "/home/agent/.local/state/actionq/handoff/aqs_old-session.md",
        "mechanism": "redispatch",
    }

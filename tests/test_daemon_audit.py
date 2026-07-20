"""Auditctl publisher integration for the daemon (item #973).

Per /projects/dev/auditctl/AGENTS.md's safety-boundary conventions, these
tests exercise a real subprocess call to a small fake ``auditctl`` binary
stand-in -- never the real production auditctl -- so the daemon's actual
``subprocess.run([...])`` argument-building and stdout/stderr handling is
covered, not just an in-process double.
"""
from __future__ import annotations

import json
import os
import stat
from pathlib import Path

from actionq.daemon import (
    ActionConfig,
    AuditConfig,
    AuditctlClient,
    Daemon,
    DaemonConfig,
    ProjectConfig,
    TakeupConfig,
)


FAKE_AUDITCTL_SCRIPT = '''#!/usr/bin/env python3
"""Fake auditctl binary stand-in for daemon integration tests.

Logs every invocation's argv to $FAKE_AUDITCTL_LOG (one JSON array per
line). Fails the first N `add` calls per event type according to a small
JSON counter file at $FAKE_AUDITCTL_FAIL_COUNTS_PATH (event_type -> count
remaining), or fails every call if $FAKE_AUDITCTL_ALWAYS_FAIL=1. On success,
emits the same JSON shape `auditctl add --json` emits.
"""
import json
import os
import sys

args = sys.argv[1:]

log_path = os.environ.get("FAKE_AUDITCTL_LOG")
if log_path:
    with open(log_path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(args) + "\\n")

if not args or args[0] != "add":
    sys.exit(2)


def _opt(name):
    return args[args.index(name) + 1] if name in args else None


event_type = _opt("--type")

if os.environ.get("FAKE_AUDITCTL_ALWAYS_FAIL") == "1":
    print("simulated permanent auditctl failure", file=sys.stderr)
    sys.exit(1)

counts_path = os.environ.get("FAKE_AUDITCTL_FAIL_COUNTS_PATH")
if counts_path and os.path.exists(counts_path):
    with open(counts_path, encoding="utf-8") as fh:
        counts = json.load(fh)
    remaining = counts.get(event_type, 0)
    if remaining > 0:
        counts[event_type] = remaining - 1
        with open(counts_path, "w", encoding="utf-8") as fh:
            json.dump(counts, fh)
        print(f"simulated transient auditctl failure for {event_type}", file=sys.stderr)
        sys.exit(1)

print(json.dumps({
    "id": "ad:00000000000000000000FAKEID",
    "event_id": "evt-fake",
    "origin_stream_id": "00000000-0000-0000-0000-000000000000",
    "origin_seq": 1,
    "ts": "2026-07-20T00:00:00Z",
    "type": event_type,
    "source": _opt("--source"),
}))
sys.exit(0)
'''


def _install_fake_auditctl(tmp_path: Path) -> Path:
    script = tmp_path / "fake-auditctl.py"
    script.write_text(FAKE_AUDITCTL_SCRIPT, encoding="utf-8")
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    return script


def _read_log(log_path: Path) -> list[list[str]]:
    if not log_path.exists():
        return []
    return [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines() if line]


def _arg(call: list[str], name: str) -> str | None:
    return call[call.index(name) + 1] if name in call else None


class FakeClient:
    def __init__(self, action=None):
        self.action = action
        self.events = []
        self.completed = []
        self.failed = []

    def claim(self, worker, timeout_minutes):
        action, self.action = self.action, None
        return action

    def emit(self, event_type, *, action_id, actor, payload):
        self.events.append((event_type, action_id, actor, payload))

    def complete(self, action_id, *, result_ref, actor):
        self.completed.append((action_id, result_ref, actor))

    def fail(self, action_id, *, reason, actor):
        self.failed.append((action_id, reason, actor))


def _daemon(tmp_path: Path, fake_bin: Path, *, action, log_path: Path, **audit_overrides) -> tuple[Daemon, FakeClient]:
    client = FakeClient(action)
    config = DaemonConfig(
        heartbeat_interval_seconds=0.01,
        session_state_path=tmp_path / "state.json",
        pause_file=tmp_path / "PAUSED",
        audit=AuditConfig(enabled=True, auditctl_bin=str(fake_bin), retry_backoff_seconds=0.01, **audit_overrides),
    )
    audit_client = AuditctlClient(str(fake_bin))
    daemon = Daemon(
        config,
        {"scope-iterate": ActionConfig(fake_duration_seconds=0.05)},
        client,
        {"demo": ProjectConfig(tmp_path, sprint_id=41)},
        audit_client=audit_client,
    )
    return daemon, client


def test_fake_auditctl_binary_receives_full_lifecycle_calls(tmp_path: Path, monkeypatch):
    fake_bin = _install_fake_auditctl(tmp_path)
    log_path = tmp_path / "audit.log"
    monkeypatch.setenv("FAKE_AUDITCTL_LOG", str(log_path))
    monkeypatch.delenv("FAKE_AUDITCTL_ALWAYS_FAIL", raising=False)
    monkeypatch.delenv("FAKE_AUDITCTL_FAIL_COUNTS_PATH", raising=False)

    daemon, client = _daemon(
        tmp_path, fake_bin,
        action={"id": 101, "action_type": "scope-iterate", "project": "demo", "target_ref": "77"},
        log_path=log_path,
    )

    assert daemon.run_once() is True

    calls = _read_log(log_path)
    event_types = [_arg(call, "--type") for call in calls]
    assert event_types == ["dispatch.queued", "session.start", "session.exit"]
    for call in calls:
        assert _arg(call, "--source") == "actionq-daemon"
        assert _arg(call, "--actor").startswith("actionq:aqs:")
        assert "--json" in call
        assert "--ref" in call
        refs = [call[i + 1] for i, arg in enumerate(call) if arg == "--ref"]
        assert "wi:77" in refs
        assert "sprint:41" in refs
        metadata = json.loads(_arg(call, "--metadata"))
        assert metadata["action_id"] == 101

    # Daemon-visible evidence mirrors what actually happened on the wire.
    dispatch_event = next(e for e in client.events if e[0] == "session.dispatch")
    started_event = next(e for e in client.events if e[0] == "session.started")
    exited_event = next(e for e in client.events if e[0] == "session.exited")
    assert dispatch_event[3]["audit_dispatch"] == {"attempted": True, "status": "ok", "event_id": "ad:00000000000000000000FAKEID", "attempts": 1}
    assert started_event[3]["audit_start"]["status"] == "ok"
    assert exited_event[3]["audit_exit"]["status"] == "ok"
    assert client.completed and client.completed[0][0] == 101
    assert not client.failed


def test_idempotent_retry_evidence_is_bounded_and_recorded(tmp_path: Path, monkeypatch):
    fake_bin = _install_fake_auditctl(tmp_path)
    log_path = tmp_path / "audit.log"
    counts_path = tmp_path / "fail-counts.json"
    counts_path.write_text(json.dumps({"dispatch.queued": 1}), encoding="utf-8")
    monkeypatch.setenv("FAKE_AUDITCTL_LOG", str(log_path))
    monkeypatch.setenv("FAKE_AUDITCTL_FAIL_COUNTS_PATH", str(counts_path))
    monkeypatch.delenv("FAKE_AUDITCTL_ALWAYS_FAIL", raising=False)

    daemon, client = _daemon(
        tmp_path, fake_bin,
        action={"id": 102, "action_type": "scope-iterate", "project": "demo", "target_ref": "78"},
        log_path=log_path, max_attempts=3,
    )

    assert daemon.run_once() is True

    calls = _read_log(log_path)
    dispatch_calls = [call for call in calls if _arg(call, "--type") == "dispatch.queued"]
    # One failed attempt, one successful retry -- bounded (not infinite),
    # and both attempts are independently visible on the wire as evidence.
    assert len(dispatch_calls) == 2

    dispatch_event = next(e for e in client.events if e[0] == "session.dispatch")
    audit_dispatch = dispatch_event[3]["audit_dispatch"]
    assert audit_dispatch["status"] == "ok"
    assert audit_dispatch["attempts"] == 2

    # Only dispatch.queued was configured to fail-then-succeed; later event
    # classes are unaffected and still succeed on the first attempt.
    started_event = next(e for e in client.events if e[0] == "session.started")
    assert started_event[3]["audit_start"] == {"attempted": True, "status": "ok", "event_id": "ad:00000000000000000000FAKEID", "attempts": 1}
    assert client.completed and client.completed[0][0] == 102


def test_audit_retry_is_bounded_not_infinite(tmp_path: Path, monkeypatch):
    fake_bin = _install_fake_auditctl(tmp_path)
    log_path = tmp_path / "audit.log"
    monkeypatch.setenv("FAKE_AUDITCTL_LOG", str(log_path))
    monkeypatch.setenv("FAKE_AUDITCTL_ALWAYS_FAIL", "1")
    monkeypatch.delenv("FAKE_AUDITCTL_FAIL_COUNTS_PATH", raising=False)

    daemon, client = _daemon(
        tmp_path, fake_bin,
        action={"id": 103, "action_type": "scope-iterate", "project": "demo", "target_ref": "79"},
        log_path=log_path, max_attempts=3,
    )

    assert daemon.run_once() is True

    calls = _read_log(log_path)
    dispatch_calls = [call for call in calls if _arg(call, "--type") == "dispatch.queued"]
    assert len(dispatch_calls) == 3  # exactly max_attempts, never more

    dispatch_event = next(e for e in client.events if e[0] == "session.dispatch")
    audit_dispatch = dispatch_event[3]["audit_dispatch"]
    assert audit_dispatch["status"] == "failed"
    assert audit_dispatch["attempts"] == 3
    assert "simulated permanent auditctl failure" in audit_dispatch["error"]


def test_audit_failure_never_blocks_the_underlying_action(tmp_path: Path, monkeypatch):
    fake_bin = _install_fake_auditctl(tmp_path)
    log_path = tmp_path / "audit.log"
    monkeypatch.setenv("FAKE_AUDITCTL_LOG", str(log_path))
    monkeypatch.setenv("FAKE_AUDITCTL_ALWAYS_FAIL", "1")
    monkeypatch.delenv("FAKE_AUDITCTL_FAIL_COUNTS_PATH", raising=False)

    daemon, client = _daemon(
        tmp_path, fake_bin,
        action={"id": 104, "action_type": "scope-iterate", "project": "demo", "target_ref": "80"},
        log_path=log_path, max_attempts=1,
    )

    assert daemon.run_once() is True

    # Every audit call failed, yet the action itself completed normally:
    # audit publication is best-effort and must never fail the underlying
    # dispatch/session action (item #973 scope).
    assert client.completed and client.completed[0][0] == 104
    assert not client.failed
    for event in client.events:
        for key in ("audit_dispatch", "audit_start", "audit_exit"):
            if key in event[3]:
                assert event[3][key]["status"] == "failed"


def test_audit_disabled_by_default_never_invokes_binary(tmp_path: Path, monkeypatch):
    fake_bin = _install_fake_auditctl(tmp_path)
    log_path = tmp_path / "audit.log"
    monkeypatch.setenv("FAKE_AUDITCTL_LOG", str(log_path))
    monkeypatch.delenv("FAKE_AUDITCTL_ALWAYS_FAIL", raising=False)

    client = FakeClient({"id": 105, "action_type": "scope-iterate", "project": "demo", "target_ref": "81"})
    config = DaemonConfig(
        heartbeat_interval_seconds=0.01,
        session_state_path=tmp_path / "state.json",
        pause_file=tmp_path / "PAUSED",
        # audit left at its default: AuditConfig(enabled=False)
        takeup=TakeupConfig(enabled=False),
    )
    daemon = Daemon(
        config, {"scope-iterate": ActionConfig(fake_duration_seconds=0.05)}, client,
        {"demo": ProjectConfig(tmp_path, sprint_id=41)},
        audit_client=AuditctlClient(str(fake_bin)),
    )

    assert daemon.run_once() is True
    assert not log_path.exists(), "the fake binary must never be invoked while audit is disabled"
    dispatch_event = next(e for e in client.events if e[0] == "session.dispatch")
    assert dispatch_event[3]["audit_dispatch"] == {"attempted": False, "status": "skipped"}


def test_pause_and_exit_audit_events_fire_on_shutdown(tmp_path: Path, monkeypatch):
    import threading

    fake_bin = _install_fake_auditctl(tmp_path)
    log_path = tmp_path / "audit.log"
    monkeypatch.setenv("FAKE_AUDITCTL_LOG", str(log_path))
    monkeypatch.delenv("FAKE_AUDITCTL_ALWAYS_FAIL", raising=False)
    monkeypatch.delenv("FAKE_AUDITCTL_FAIL_COUNTS_PATH", raising=False)

    client = FakeClient({"id": 106, "action_type": "scope-iterate", "project": "demo", "target_ref": "82"})
    config = DaemonConfig(
        graceful_shutdown_seconds=0.01,
        session_state_path=tmp_path / "state.json",
        pause_file=tmp_path / "PAUSED",
        audit=AuditConfig(enabled=True, auditctl_bin=str(fake_bin), retry_backoff_seconds=0.01),
    )
    daemon = Daemon(
        config, {"scope-iterate": ActionConfig(fake_duration_seconds=10)}, client,
        {"demo": ProjectConfig(tmp_path, sprint_id=41)},
        audit_client=AuditctlClient(str(fake_bin)),
    )

    started = threading.Event()
    original_emit = client.emit

    def _emit(event_type, *, action_id, actor, payload):
        original_emit(event_type, action_id=action_id, actor=actor, payload=payload)
        if event_type == "session.started":
            started.set()

    client.emit = _emit

    worker = threading.Thread(target=daemon.run_once)
    worker.start()
    assert started.wait(timeout=2)
    daemon.request_shutdown()
    worker.join(timeout=2)

    assert not worker.is_alive()
    calls = _read_log(log_path)
    event_types = [_arg(call, "--type") for call in calls]
    assert "session.pause" in event_types
    assert event_types[-1] == "session.exit"

    paused_event = next(e for e in client.events if e[0] == "session.paused")
    assert paused_event[3]["audit_pause"]["status"] == "ok"
    exited_event = next(e for e in client.events if e[0] == "session.exited")
    assert exited_event[3]["outcome"] == "shutdown"
    assert exited_event[3]["audit_exit"]["status"] == "ok"

"""Tier-1 deterministic context injection at session start (item #1116).

Exercises ``Daemon._context_candidates_request`` /
``Daemon._context_claim_acquire`` wiring in ``_run_action``: a bounded
sprintctl ``context-candidates`` packet is requested before the child starts
(best-effort, fail-open), and a pre-start claim is only ever attempted for an
explicit target sprintctl itself marked ``claim_eligible`` -- never for an
advisory/inferred candidate -- with that attempt failing closed. See
``sprintctl/docs/ops-upgrade-plan.md`` Tier 1 and
``agentops/docs/plans/agentops/session-mechanization-plan.md`` Tier 1.
"""
from __future__ import annotations

from pathlib import Path

from actionq.daemon import ActionConfig, ContextConfig, Daemon, DaemonConfig, ProjectConfig, load_config

from tests.test_daemon import FakeClient


class FakeContext:
    def __init__(self, packet=None, fail: bool = False):
        self.calls = []
        self.packet = packet
        self.fail = fail

    def fetch(self, project, *, item_id, limit):
        self.calls.append((project, item_id, limit))
        if self.fail:
            raise RuntimeError("sprintctl context-candidates: connection refused")
        return self.packet


class FakeClaim:
    def __init__(self, fail: bool = False, response=None):
        self.calls = []
        self.fail = fail
        self.response = response if response is not None else {"claim_id": 900}

    def start(self, project, *, item_id, actor, ttl_seconds, branch):
        self.calls.append((project, item_id, actor, ttl_seconds, branch))
        if self.fail:
            raise RuntimeError("sprintctl claim start: item already active")
        return self.response


def _remote_project(tmp_path: Path, sprint_id: int = 7) -> ProjectConfig:
    return ProjectConfig(tmp_path, sprint_id=sprint_id, env={"SPRINTCTL_BACKEND": "remote"})


def _packet(*, explicit_item_id=None, found=False, eligible_rank1=False, extra_candidates=()):
    candidates = list(extra_candidates)
    if explicit_item_id is not None and found:
        candidates.insert(0, {"item_id": explicit_item_id, "rank": 1, "claim_eligible": eligible_rank1})
    return {
        "contract_version": "1",
        "explicit_target": ({"item_id": explicit_item_id, "found": found} if explicit_item_id is not None else None),
        "bound": 5,
        "truncated": False,
        "watermark": {"ingest_offset": 42, "age_seconds": 12.3},
        "candidates": candidates,
    }


def test_context_disabled_by_default_never_calls_context_or_claim(tmp_path: Path):
    client = FakeClient({"id": 40, "action_type": "scope-iterate", "project": "demo", "target_ref": "5"})
    context, claim = FakeContext(_packet(explicit_item_id=5, found=True, eligible_rank1=True)), FakeClaim()
    daemon = Daemon(
        DaemonConfig(session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED"),
        {"scope-iterate": ActionConfig(fake_duration_seconds=0.01)}, client,
        {"demo": _remote_project(tmp_path)}, context_client=context, claim_client=claim,
    )

    assert daemon.run_once() is True
    assert context.calls == []
    assert claim.calls == []
    dispatch_payload = client.events[0][3]
    assert dispatch_payload["context"] is None
    assert dispatch_payload["context_claim"] is None
    assert client.completed and client.completed[0][0] == 40


def test_context_enabled_skips_for_local_project(tmp_path: Path):
    client = FakeClient({"id": 41, "action_type": "scope-iterate", "project": "demo", "target_ref": "5"})
    context, claim = FakeContext(_packet(explicit_item_id=5, found=True, eligible_rank1=True)), FakeClaim()
    daemon = Daemon(
        DaemonConfig(
            session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED",
            context=ContextConfig(enabled=True),
        ),
        {"scope-iterate": ActionConfig(fake_duration_seconds=0.01)}, client,
        {"demo": ProjectConfig(tmp_path, sprint_id=7, env={})},  # no SPRINTCTL_BACKEND=remote
        context_client=context, claim_client=claim,
    )

    assert daemon.run_once() is True
    assert context.calls == []
    assert claim.calls == []
    assert client.events[0][3]["context"] == {"attempted": False, "status": "skipped", "reason": "local-mode"}


def test_non_explicit_candidates_are_advisory_only_no_claim(tmp_path: Path):
    client = FakeClient({"id": 42, "action_type": "scope-iterate", "project": "demo", "target_ref": "5"})
    packet = _packet(
        explicit_item_id=5, found=False,
        extra_candidates=[{"item_id": 99, "rank": 2, "claim_eligible": False}],
    )
    context, claim = FakeContext(packet), FakeClaim()
    daemon = Daemon(
        DaemonConfig(
            session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED",
            context=ContextConfig(enabled=True),
        ),
        {"scope-iterate": ActionConfig(fake_duration_seconds=0.01)}, client,
        {"demo": _remote_project(tmp_path)}, context_client=context, claim_client=claim,
    )

    assert daemon.run_once() is True
    assert [call[1] for call in context.calls] == [5]
    assert claim.calls == []
    dispatch_payload = client.events[0][3]
    assert dispatch_payload["context"] == {"attempted": True, "status": "ok", "packet": packet}
    assert dispatch_payload["context_claim"] is None
    event_types = [event[0] for event in client.events]
    assert "session.started" in event_types
    assert client.completed and client.completed[0][0] == 42


def test_explicit_eligible_target_acquires_pre_start_claim(tmp_path: Path):
    client = FakeClient({"id": 43, "action_type": "scope-iterate", "project": "demo", "target_ref": "5"})
    packet = _packet(explicit_item_id=5, found=True, eligible_rank1=True)
    context = FakeContext(packet)
    claim = FakeClaim(response={"claim_id": 901})
    daemon = Daemon(
        DaemonConfig(
            session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED",
            context=ContextConfig(enabled=True),
        ),
        {"scope-iterate": ActionConfig(fake_duration_seconds=0.01)}, client,
        {"demo": _remote_project(tmp_path)}, context_client=context, claim_client=claim,
    )

    assert daemon.run_once() is True
    assert len(claim.calls) == 1
    _project, item_id, actor, ttl_seconds, branch = claim.calls[0]
    assert item_id == 5
    assert actor == f"actionq:{client.events[0][3]['session_id']}"
    assert ttl_seconds == 1800
    assert branch is None
    dispatch_payload = client.events[0][3]
    assert dispatch_payload["context_claim"] == {"attempted": True, "status": "ok", "item_id": 5, "claim_id": 901}
    assert client.completed and client.completed[0][0] == 43


def test_claim_acquisition_failure_fails_closed_before_child_starts(tmp_path: Path):
    client = FakeClient({"id": 44, "action_type": "scope-iterate", "project": "demo", "target_ref": "5"})
    packet = _packet(explicit_item_id=5, found=True, eligible_rank1=True)
    context, claim = FakeContext(packet), FakeClaim(fail=True)
    daemon = Daemon(
        DaemonConfig(
            session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED",
            context=ContextConfig(enabled=True),
        ),
        {"scope-iterate": ActionConfig(fake_duration_seconds=10)}, client,
        {"demo": _remote_project(tmp_path)}, context_client=context, claim_client=claim,
    )

    claimed = daemon.run_once()

    assert claimed is True
    assert len(claim.calls) == 1
    assert "session.started" not in [event[0] for event in client.events]
    assert client.failed and client.failed[0][0] == 44
    assert "context claim acquisition failed before session start" in client.failed[0][1]
    assert not client.completed
    assert daemon._child is None
    assert client.events[0][3]["context_claim"]["status"] == "failed"
    # The failure happens before ``_start_child``/``_write_state`` are ever
    # reached, so no session-state file gets created at all -- there is no
    # "started then cleared" cycle to observe here, unlike the takeup
    # pre-start failure path which starts the child before failing.
    assert not daemon.config.session_state_path.exists()


def test_context_fetch_failure_is_advisory_and_session_still_starts(tmp_path: Path):
    client = FakeClient({"id": 45, "action_type": "scope-iterate", "project": "demo", "target_ref": "5"})
    context, claim = FakeContext(fail=True), FakeClaim()
    daemon = Daemon(
        DaemonConfig(
            session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED",
            context=ContextConfig(enabled=True),
        ),
        {"scope-iterate": ActionConfig(fake_duration_seconds=0.01)}, client,
        {"demo": _remote_project(tmp_path)}, context_client=context, claim_client=claim,
    )

    assert daemon.run_once() is True
    assert claim.calls == []
    dispatch_payload = client.events[0][3]
    assert dispatch_payload["context"]["status"] == "failed"
    assert "connection refused" in dispatch_payload["context"]["error"]
    assert "session.started" in [event[0] for event in client.events]
    assert client.completed and client.completed[0][0] == 45


def test_auto_claim_disabled_skips_claim_even_when_eligible(tmp_path: Path):
    client = FakeClient({"id": 46, "action_type": "scope-iterate", "project": "demo", "target_ref": "5"})
    packet = _packet(explicit_item_id=5, found=True, eligible_rank1=True)
    context, claim = FakeContext(packet), FakeClaim()
    daemon = Daemon(
        DaemonConfig(
            session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED",
            context=ContextConfig(enabled=True, auto_claim=False),
        ),
        {"scope-iterate": ActionConfig(fake_duration_seconds=0.01)}, client,
        {"demo": _remote_project(tmp_path)}, context_client=context, claim_client=claim,
    )

    assert daemon.run_once() is True
    assert claim.calls == []
    assert client.events[0][3]["context_claim"] is None
    assert client.completed and client.completed[0][0] == 46


def test_load_config_reads_context_settings(tmp_path: Path):
    config_path = tmp_path / "daemon.toml"
    config_path.write_text(
        "[global]\n"
        "session_state_path = 'state.json'\n"
        "sprintctl_bin = 'sprintctl-devbox'\n"
        "[global.context]\n"
        "enabled = true\n"
        "remote_only = false\n"
        "limit = 3\n"
        "auto_claim = false\n"
    )

    config, _actions, _projects = load_config(config_path)

    assert config.context.enabled is True
    assert config.context.remote_only is False
    assert config.context.sprintctl_bin == "sprintctl-devbox"
    assert config.context.limit == 3
    assert config.context.auto_claim is False

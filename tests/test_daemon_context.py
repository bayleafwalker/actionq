"""Tier-1 deterministic context injection at session start (item #1116).

Exercises ``Daemon._context_candidates_request`` /
``Daemon._context_reservation_acquire`` wiring in ``_run_action``: a bounded
sprintctl ``context-candidates`` packet is requested before the child starts
(best-effort, fail-open), and a pre-start reservation is only ever attempted
for an explicit target sprintctl itself marked ``reservation_admissible`` --
never for an advisory/inferred candidate -- with that attempt failing closed. See
``sprintctl/docs/ops-upgrade-plan.md`` Tier 1 and
``agentops/docs/plans/agentops/session-mechanization-plan.md`` Tier 1.
"""
from __future__ import annotations

from pathlib import Path
import subprocess

from actionq.daemon import (
    ActionConfig, ContextConfig, Daemon, DaemonConfig, ProjectConfig, load_config,
)
from actionq.routing import HarnessRoute, RoutingContext
from actionq.scope_iterate import PathACL, ScopeIteratePolicy, ToolACL

from tests.test_daemon import FakeClient


class FakeContext:
    def __init__(self, packet=None, fail: bool = False, item=None):
        self.calls = []
        self.item_calls = []
        self.packet = packet
        self.fail = fail
        self.item = item

    def fetch(self, project, *, item_id, limit):
        self.calls.append((project, item_id, limit))
        if self.fail:
            raise RuntimeError("sprintctl context-candidates: connection refused")
        return self.packet

    def fetch_item(self, project, *, item_id):
        self.item_calls.append((project, item_id))
        if self.fail:
            raise RuntimeError("sprintctl item show: connection refused")
        return self.item or {
            "id": item_id,
            "title": "Exact item",
            "description": "Implement the exact requested change.",
            "status": "pending",
        }


class FakeClaim:
    def __init__(self, fail: bool = False, response=None):
        self.calls = []
        self.fail = fail
        self.response = response if response is not None else {"id": 900, "conflict": False}
        self.touch_calls = []
        self.release_calls = []
        self.touch_error = None
        self.release_error = None

    def reserve(self, project, *, item_id, actor, role, session_id, correlation_ref):
        self.calls.append((project, item_id, actor, role, session_id, correlation_ref))
        if self.fail:
            raise RuntimeError("sprintctl reservation reserve: backend unavailable")
        return self.response

    def touch(self, project, *, reservation_id, session_id, correlation_ref):
        self.touch_calls.append((project, reservation_id, session_id, correlation_ref))
        if self.touch_error is not None:
            raise self.touch_error
        return {"id": reservation_id, "state": "active"}

    def release(self, project, *, reservation_id, actor):
        self.release_calls.append((project, reservation_id, actor))
        if self.release_error is not None:
            raise self.release_error
        return {"id": reservation_id, "state": "released"}


def _remote_project(tmp_path: Path, sprint_id: int = 7) -> ProjectConfig:
    return ProjectConfig(tmp_path, sprint_id=sprint_id, env={"SPRINTCTL_BACKEND": "remote"})


def _packet(*, explicit_item_id=None, found=False, eligible_rank1=False, extra_candidates=()):
    candidates = list(extra_candidates)
    if explicit_item_id is not None and found:
        candidates.insert(0, {"item_id": explicit_item_id, "rank": 1, "reservation_admissible": eligible_rank1})
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
    assert dispatch_payload["context_reservation"] is None
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


def test_context_remote_only_accepts_served_backend(tmp_path: Path):
    client = FakeClient({"id": 141, "action_type": "scope-iterate", "project": "demo", "target_ref": "5"})
    context = FakeContext(_packet(explicit_item_id=5, found=True, eligible_rank1=False))
    daemon = Daemon(
        DaemonConfig(
            session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED",
            context=ContextConfig(enabled=True),
        ),
        {"scope-iterate": ActionConfig(fake_duration_seconds=0.01)}, client,
        {"demo": ProjectConfig(tmp_path, sprint_id=7, env={"SPRINTCTL_BACKEND": "served"})},
        context_client=context,
    )

    assert daemon.run_once() is True
    assert [call[1] for call in context.calls] == [5]
    assert client.completed


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ("git", *args), cwd=repo, check=True, text=True, capture_output=True
    ).stdout.strip()


def _scope_policy(tmp_path: Path) -> ScopeIteratePolicy:
    prompt = tmp_path / "prompt.md"
    prompt.write_text("{action_json}\n{sprint_item_json}\n{working_dir}\n{branch_name}\n"
                      "{allowed_scope}\n{test_command}\n")
    return ScopeIteratePolicy(
        worktree_root=(tmp_path / "worktrees").resolve(),
        prompt_template=prompt.resolve(),
        path_acl=PathACL(
            ("docs/**",),
            (".git/**", ".sprintctl/**", "secrets/**", "**/.env", "**/.env.*"),
        ),
        tool_acl=ToolACL(),
        test_command=("python3", "-c", "pass"),
    )


def test_scope_iterate_claims_exact_target_branch_and_settles_verified_commit(tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "config", "user.email", "test@example.invalid")
    (repo / "README.md").write_text("# demo\n")
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-m", "initial")
    client = FakeClient({
        "id": 142, "action_type": "scope-iterate", "project": "demo",
        "target_ref": "5", "harness": "codex", "model": "test-model",
    })
    context = FakeContext(
        _packet(explicit_item_id=5, found=True, eligible_rank1=True),
        item={
            "id": 5,
            "title": "Exact item",
            "description": "Create the governed smoke artifact.",
            "status": "pending",
        },
    )
    claim = FakeClaim()
    daemon = Daemon(
            DaemonConfig(
                heartbeat_interval_seconds=0.01,
                enforce_worker_isolation=False,
            session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED",
            context=ContextConfig(enabled=True),
            routing=RoutingContext(harnesses={"codex": HarnessRoute("codex")}),
        ),
        {"scope-iterate": ActionConfig(
            runner="scope-iterate", harness="codex", model="test-model",
            scope_iterate=_scope_policy(tmp_path),
        )},
        client,
        {"demo": ProjectConfig(
            repo, sprint_id=7, env={"SPRINTCTL_BACKEND": "served"},
        )},
        context_client=context, claim_client=claim,
    )

    captured = {}

    def start_child(_action, *, project, **_kwargs):
        captured["prompt"] = _kwargs["prompt"]
        script = (
            "from pathlib import Path; import subprocess; "
            "p=Path('docs/unit-b.md'); p.parent.mkdir(parents=True, exist_ok=True); "
            "p.write_text('verified\\n'); "
            "subprocess.run(['git','add','--','docs/unit-b.md'],check=True); "
            "subprocess.run(['git','-c','user.name=Worker','-c',"
            "'user.email=worker@example.invalid','commit','-m','unit b'],check=True)"
        )
        return subprocess.Popen(
            ["python3", "-c", script], cwd=project.path, text=True,
            start_new_session=True,
        )

    daemon._start_child = start_child
    assert daemon.run_once() is True

    assert not client.failed, client.failed
    assert claim.calls[0][1] == 5
    assert context.item_calls[0][1] == 5
    assert "Create the governed smoke artifact." in captured["prompt"]
    assert claim.calls[0][3] == "execution"
    assert claim.calls[0][4] == client.events[0][3]["session_id"]
    assert len(claim.release_calls) == 1
    assert client.completed
    assert client.settled[0][1]["terminal_status"] == "completed"
    assert client.settled[0][1]["attempt_id"] == client.events[0][3]["session_id"]
    event_types = [event[0] for event in client.events]
    assert event_types.index("settlement.sprint_reservation_released") < len(event_types)


def test_scope_iterate_rejects_context_target_mismatch_before_claim(tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    client = FakeClient({
        "id": 143, "action_type": "scope-iterate", "project": "demo",
        "target_ref": "5", "harness": "codex", "model": "test-model",
    })
    context = FakeContext(_packet(explicit_item_id=6, found=True, eligible_rank1=True))
    claim = FakeClaim()
    daemon = Daemon(
            DaemonConfig(
                enforce_worker_isolation=False,
            session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED",
            context=ContextConfig(enabled=True),
            routing=RoutingContext(harnesses={"codex": HarnessRoute("codex")}),
        ),
        {"scope-iterate": ActionConfig(
            runner="scope-iterate", harness="codex", model="test-model",
            scope_iterate=_scope_policy(tmp_path),
        )},
        client,
        {"demo": ProjectConfig(repo, sprint_id=7, env={"SPRINTCTL_BACKEND": "served"})},
        context_client=context, claim_client=claim,
    )

    assert daemon.run_once() is True
    assert claim.calls == []
    assert client.completed == []
    assert client.failed[0][1] == "start-failed"


def test_non_explicit_candidates_are_advisory_only_no_claim(tmp_path: Path):
    client = FakeClient({"id": 42, "action_type": "scope-iterate", "project": "demo", "target_ref": "5"})
    packet = _packet(
        explicit_item_id=5, found=False,
        extra_candidates=[{"item_id": 99, "rank": 2, "reservation_admissible": False}],
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
    assert dispatch_payload["context_reservation"] is None
    event_types = [event[0] for event in client.events]
    assert "session.started" in event_types
    assert client.completed and client.completed[0][0] == 42


def test_explicit_admissible_target_acquires_pre_start_reservation(tmp_path: Path):
    client = FakeClient({"id": 43, "action_type": "scope-iterate", "project": "demo", "target_ref": "5"})
    packet = _packet(explicit_item_id=5, found=True, eligible_rank1=True)
    context = FakeContext(packet)
    claim = FakeClaim(response={"id": 901, "conflict": True, "conflict_severity": "warning"})
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
    _project, item_id, actor, role, session_id, correlation_ref = claim.calls[0]
    assert item_id == 5
    assert actor == f"actionq:{client.events[0][3]['session_id']}"
    assert role == "execution"
    assert session_id == client.events[0][3]["session_id"]
    assert correlation_ref == f"actionq:{session_id}"
    dispatch_payload = client.events[0][3]
    assert dispatch_payload["context_reservation"] == {
        "attempted": True, "status": "ok", "item_id": 5, "reservation_id": 901,
        "conflict": True, "conflict_severity": "warning", "conflicting_reservations": [],
    }
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
    assert client.failed[0][1] == "start-failed"
    assert not client.completed
    assert daemon._child is None
    assert client.events[0][3]["context_reservation"]["status"] == "failed"
    # The failure happens before ``_start_child``/``_write_state`` are ever
    # reached, so no session-state file gets created at all -- there is no
    # "started then cleared" cycle to observe here, unlike the takeup
    # pre-start failure path which starts the child before failing.
    assert not daemon.config.session_state_path.exists()


def test_reservation_without_public_id_fails_closed_before_child_starts(tmp_path: Path):
    client = FakeClient({"id": 47, "action_type": "scope-iterate", "project": "demo", "target_ref": "5"})
    context = FakeContext(_packet(explicit_item_id=5, found=True, eligible_rank1=True))
    claim = FakeClaim(response={"conflict": False})
    daemon = Daemon(
        DaemonConfig(session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED", context=ContextConfig(enabled=True)),
        {"scope-iterate": ActionConfig(fake_duration_seconds=0.01)}, client,
        {"demo": _remote_project(tmp_path)}, context_client=context, claim_client=claim,
    )

    assert daemon.run_once() is True
    assert "session.started" not in [event[0] for event in client.events]
    assert client.failed and client.failed[0][1] == "start-failed"


def test_supervision_touches_sprint_reservation_without_emitting_credentials(tmp_path: Path):
    client = FakeClient({"id": 48, "action_type": "scope-iterate", "project": "demo", "target_ref": "5"})
    context = FakeContext(_packet(explicit_item_id=5, found=True, eligible_rank1=True))
    claim = FakeClaim()
    daemon = Daemon(
        DaemonConfig(
            heartbeat_interval_seconds=0.01, session_state_path=tmp_path / "state.json",
            pause_file=tmp_path / "PAUSED", context=ContextConfig(enabled=True),
        ),
        {"scope-iterate": ActionConfig(fake_duration_seconds=0.12)}, client,
        {"demo": _remote_project(tmp_path)}, context_client=context, claim_client=claim,
    )

    assert daemon.run_once() is True
    assert claim.touch_calls
    assert claim.release_calls
    rendered_events = repr(client.events)
    assert "claim_token" not in rendered_events


def test_sprint_reservation_touch_loss_does_not_stop_actionq_execution(tmp_path: Path):
    client = FakeClient({"id": 49, "action_type": "scope-iterate", "project": "demo", "target_ref": "5"})
    context = FakeContext(_packet(explicit_item_id=5, found=True, eligible_rank1=True))
    claim = FakeClaim()
    claim.touch_error = RuntimeError("sprintctl reservation touch unavailable")
    daemon = Daemon(
        DaemonConfig(
            heartbeat_interval_seconds=0.01, session_state_path=tmp_path / "state.json",
            pause_file=tmp_path / "PAUSED", context=ContextConfig(enabled=True),
        ),
        {"scope-iterate": ActionConfig(fake_duration_seconds=5)}, client,
        {"demo": _remote_project(tmp_path)}, context_client=context, claim_client=claim,
    )

    assert daemon.run_once() is True
    assert claim.touch_calls
    assert client.completed
    assert not client.failed
    assert "session.reservation_touch_failed" in [event[0] for event in client.events]


def test_sprint_reservation_release_failure_is_visible_but_does_not_block_settlement(tmp_path: Path):
    client = FakeClient({"id": 50, "action_type": "scope-iterate", "project": "demo", "target_ref": "5"})
    context = FakeContext(_packet(explicit_item_id=5, found=True, eligible_rank1=True))
    claim = FakeClaim()
    claim.release_error = RuntimeError("sprintctl unavailable")
    daemon = Daemon(
        DaemonConfig(session_state_path=tmp_path / "state.json", pause_file=tmp_path / "PAUSED", context=ContextConfig(enabled=True)),
        {"scope-iterate": ActionConfig(fake_duration_seconds=0.01)}, client,
        {"demo": _remote_project(tmp_path)}, context_client=context, claim_client=claim,
    )

    assert daemon.run_once() is True
    assert claim.release_calls
    assert client.completed
    assert not client.failed
    event_types = [event[0] for event in client.events]
    assert "settlement.pending" in event_types
    assert "settlement.sprint_reservation_release_failed" in event_types
    assert client.events[-1][0] == "session.exited"


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
    assert client.events[0][3]["context_reservation"] is None
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

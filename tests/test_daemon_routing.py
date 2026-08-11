from __future__ import annotations

import json
from pathlib import Path

import pytest

from actionq.completion_outbox import CompletionOutbox

from actionq.daemon import ActionConfig, Daemon, DaemonConfig, ProjectConfig, load_config
from actionq.routing import HarnessRoute, RoutingContext, RoutingResult

from tests.test_daemon import FakeClient


def _policy(tmp_path: Path) -> Path:
    path = tmp_path / "model-routing.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "caller_harness_providers": {"claude": "anthropic", "codex": "codex"},
                "aliases": {
                    "fast-build": {
                        "anthropic": {"model": "claude-haiku", "verified": True},
                        "codex": {
                            "model": "gpt-spark",
                            "fallback": "gpt-luna",
                            "verified": True,
                            "transport": "chatgpt",
                            "surfaces": ["codex-cli"],
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def _fake_codex(tmp_path: Path, *, exit_code: int = 0, output: str = "ok") -> Path:
    path = tmp_path / "fake-codex"
    path.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        "if sys.argv[1:] == ['debug', 'models']:\n"
        "    print(json.dumps({'models':[\n"
        "        {'slug':'gpt-5.6-sol','multi_agent_version':'v2'},\n"
        "        {'slug':'gpt-5.6-luna','multi_agent_version':'v1'},\n"
        "    ]}))\n"
        "    raise SystemExit(0)\n"
        f"capture = {str(tmp_path / 'capture.json')!r}\n"
        "if capture:\n"
        "    with open(capture, 'w', encoding='utf-8') as handle:\n"
        "        argv = sys.argv[1:]\n"
        "        catalog_path = json.loads(argv[1].split('=', 1)[1]) if argv[:1] == ['-c'] else None\n"
        "        catalog = json.load(open(catalog_path, encoding='utf-8')) if catalog_path else None\n"
        "        json.dump({'argv': argv, 'stdin': sys.stdin.read(), 'catalog_path': catalog_path,\n"
        "                   'catalog': catalog}, handle)\n"
        f"print({output!r})\n"
        f"raise SystemExit({exit_code})\n",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def _daemon(
    tmp_path: Path,
    client: FakeClient,
    binary: Path,
    *,
    trusted_caller_harness: str | None = "codex",
    catalog_workaround: str | None = None,
) -> Daemon:
    routing = RoutingContext(
        policy_path=_policy(tmp_path),
        default_harness="caller",
        trusted_caller_harness=trusted_caller_harness,
        caller_transport="chatgpt",
        caller_surface="codex-cli",
        harnesses={
            "claude": HarnessRoute("claude"),
            "codex": HarnessRoute(
                "codex",
                bin=str(binary),
                provider="codex",
                transport="chatgpt",
                surface="codex-cli",
                catalog_workaround=catalog_workaround,
            ),
        },
    )
    config = DaemonConfig(
        enforce_worker_isolation=False,
        session_state_path=tmp_path / "state.json",
        pause_file=tmp_path / "PAUSED",
        handoff_dir=tmp_path / "handoff",
        routing=routing,
    )
    actions = {
        "scope-iterate": ActionConfig(
            runner="harness",
            harness="caller",
            model="fast-build",
            prompt="Run the required targeted tests.",
        )
    }
    projects = {
        "demo": ProjectConfig(
            path=tmp_path,
            env={},
            default_harness="caller",
        )
    }
    return Daemon(config, actions, client, projects)


def test_harness_runner_resolves_caller_and_records_lifecycle_provenance(tmp_path: Path):
    client = FakeClient({"id": 40, "action_type": "scope-iterate", "project": "demo"})
    daemon = _daemon(tmp_path, client, _fake_codex(tmp_path))

    assert daemon.run_once() is True

    capture = json.loads((tmp_path / "capture.json").read_text(encoding="utf-8"))
    assert capture["stdin"] == "Run the required targeted tests."
    assert capture["argv"][:2] == ["exec", "--skip-git-repo-check"]
    model_index = capture["argv"].index("--model")
    assert capture["argv"][model_index + 1] == "gpt-spark"

    lifecycle = {
        event_type: payload
        for event_type, _action_id, _actor, payload in client.events
        if event_type in {"session.dispatch", "session.started", "session.exited"}
    }
    assert set(lifecycle) == {"session.dispatch", "session.started", "session.exited"}
    for payload in lifecycle.values():
        assert payload["routing"]["trusted_caller_harness"] == "codex"
        assert payload["routing"]["resolved_provider"] == "codex"
        assert payload["routing"]["resolved_model"] == "gpt-spark"
        assert payload["routing"]["routing_source"] == "caller-inheritance"
    assert client.completed and not client.failed


def test_harness_runner_applies_catalog_workaround_and_records_only_named_provenance(tmp_path: Path):
    client = FakeClient({"id": 43, "action_type": "scope-iterate", "project": "demo"})
    daemon = _daemon(
        tmp_path,
        client,
        _fake_codex(tmp_path),
        catalog_workaround="luna-v1-to-v2",
    )

    assert daemon.run_once() is True

    capture = json.loads((tmp_path / "capture.json").read_text(encoding="utf-8"))
    assert capture["argv"][0] == "-c"
    assert capture["catalog"]["models"][1]["multi_agent_version"] == "v2"
    assert not Path(capture["catalog_path"]).exists()
    lifecycle = [
        payload
        for event_type, _action_id, _actor, payload in client.events
        if event_type in {"session.dispatch", "session.started", "session.exited"}
    ]
    assert lifecycle
    for payload in lifecycle:
        assert payload["routing"]["catalog_workaround"] == "luna-v1-to-v2"
        serialized = json.dumps(payload["routing"], sort_keys=True)
        assert "model_catalog_json" not in serialized
        assert "actionq-codex-models-" not in serialized


def test_routing_error_rejects_before_any_child_or_lifecycle_event(tmp_path: Path):
    client = FakeClient({"id": 41, "action_type": "scope-iterate", "project": "demo"})
    daemon = _daemon(
        tmp_path,
        client,
        _fake_codex(tmp_path),
        trusted_caller_harness=None,
    )

    assert daemon.run_once() is True

    assert not (tmp_path / "capture.json").exists()
    assert client.events == []
    assert client.failed[0][0] == 41
    assert client.failed[0][1] == "start-failed"


def test_spark_limit_writes_same_provider_luna_redispatch_handoff(tmp_path: Path):
    client = FakeClient({"id": 42, "action_type": "scope-iterate", "project": "demo"})
    binary = _fake_codex(tmp_path, exit_code=1, output="429 too many requests")
    daemon = _daemon(tmp_path, client, binary)

    assert daemon.run_once() is True

    paused = next(payload for event, _id, _actor, payload in client.events if event == "session.paused")
    redispatch = paused["redispatch_routing"]
    assert redispatch["resolved_harness"] == "codex"
    assert redispatch["resolved_provider"] == "codex"
    assert redispatch["resolved_model"] == "gpt-luna"
    assert redispatch["routing_source"] == "same-provider-fallback"

    handoff = Path(paused["handoff_ref"]).read_text(encoding="utf-8")
    assert "redispatch_harness: codex" in handoff
    assert "redispatch_provider: codex" in handoff
    assert "redispatch_model: gpt-luna" in handoff
    assert client.failed[0][1] == "usage-limit"
    completion = CompletionOutbox(
        daemon.config.session_state_path.parent / "completion-outbox.sqlite3"
    ).pending()[0]["payload"]
    assert completion["terminal"]["kind"] == "usage-limited"


def test_load_config_reads_trusted_routing_harness_and_action_fields(tmp_path: Path):
    policy = _policy(tmp_path)
    config_path = tmp_path / "actionq.toml"
    config_path.write_text(
        "[routing]\n"
        f"policy_path = {str(policy)!r}\n"
        "default_harness = 'caller'\n"
        "trusted_caller_harness = 'codex'\n"
        "caller_provider = 'codex'\n"
        "caller_transport = 'chatgpt'\n"
        "caller_surface = 'codex-cli'\n"
        "[harnesses.codex]\n"
        "bin = '/opt/bin/codex'\n"
        "provider = 'codex'\n"
        "transport = 'chatgpt'\n"
        "surface = 'codex-cli'\n"
        "catalog_workaround = 'luna-v1-to-v2'\n"
        "[projects.demo]\n"
        f"path = {str(tmp_path)!r}\n"
        "default_harness = 'caller'\n"
        "default_model = 'fast-build'\n"
        "[actions.scope-iterate]\n"
        "runner = 'harness'\n"
        "harness = 'caller'\n"
        "model = 'fast-build'\n"
        "worker_user = 'agentworker'\n"
        "harness_profile = 'opencode-nixpkgs-devbox-1.18.4'\n"
        "prompt = 'Run tests.'\n",
        encoding="utf-8",
    )

    config, actions, projects = load_config(config_path)

    assert config.routing.policy_path == policy
    assert config.routing.trusted_caller_harness == "codex"
    assert config.routing.harnesses["codex"].bin == "/opt/bin/codex"
    assert config.routing.harnesses["codex"].catalog_workaround == "luna-v1-to-v2"
    assert actions["scope-iterate"] == ActionConfig(
        runner="harness", harness="caller", model="fast-build", worker_user="agentworker",
        harness_profile="opencode-nixpkgs-devbox-1.18.4", prompt="Run tests."
    )
    assert projects["demo"].default_model == "fast-build"


@pytest.mark.parametrize(
    ("harness", "workaround"),
    [
        ("codex", "unknown-catalog-edit"),
        ("codex", ""),
        ("claude", "luna-v1-to-v2"),
    ],
)
def test_load_config_rejects_unapproved_catalog_workarounds(
    tmp_path: Path,
    harness: str,
    workaround: str,
):
    config_path = tmp_path / "actionq.toml"
    config_path.write_text(
        f"[harnesses.{harness}]\n"
        f"catalog_workaround = {workaround!r}\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unsupported catalog_workaround"):
        load_config(config_path)


def test_scope_iterate_worker_runs_through_contained_identity(tmp_path: Path, monkeypatch):
    daemon = Daemon(DaemonConfig(), {}, FakeClient())
    action = ActionConfig(runner="scope-iterate", worker_user="agentworker")
    project = ProjectConfig(tmp_path)
    routing = RoutingResult(
        requested_selector="gpt-test", caller_harness=None, harness="codex",
        provider="codex", model="gpt-test", transport=None, surface=None,
        routing_source="test",
    )
    captured = {}

    class Input:
        def write(self, value):
            captured["packet"] = value
        def close(self):
            pass

    class Child:
        stdin = Input()

    def fake_popen(command, **kwargs):
        captured["command"] = command
        return Child()

    monkeypatch.setattr("actionq.daemon.subprocess.Popen", fake_popen)
    monkeypatch.setattr("actionq.daemon.shutil.which", lambda name: "/run/wrappers/bin/sudo" if name == "sudo" else None)
    class Adapter:
        def build_command(self, _invocation):
            return ["/run/current-system/sw/bin/opencode", "run"]

        def build_env(self, _invocation):
            return {"OPENCODE_CONFIG": "/etc/agentops/opencode.actionq-worker.json", "SECRET": "coordinator-only"}

        def stdin_text(self, _invocation):
            return None

    monkeypatch.setattr("actionq.daemon.get_adapter", lambda *_args, **_kwargs: Adapter())

    from actionq_contracts import EXECUTION_ENVELOPE_V1, ExecutionEnvelope
    daemon._start_child(
        action, project=project, routing=routing, prompt="work",
        envelope=ExecutionEnvelope(EXECUTION_ENVELOPE_V1, 1, "attempt-1", "abcdef1", "test:harness"),
    )

    assert captured["command"] == ["actionq-runner", "execute"]
    packet = json.loads(captured["packet"])
    assert packet["command"][:7] == [
        "/run/wrappers/bin/sudo", "-n", "-H", "--preserve-env=OPENCODE_CONFIG", "-u", "agentworker", "--",
    ]
    assert packet["environment"] == {"OPENCODE_CONFIG": "/etc/agentops/opencode.actionq-worker.json"}

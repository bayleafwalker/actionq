from __future__ import annotations

import json
from pathlib import Path

from actionq.daemon import ActionConfig, Daemon, DaemonConfig, ProjectConfig, load_config
from actionq.routing import HarnessRoute, RoutingContext

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
        "capture = os.environ.get('CAPTURE_PATH')\n"
        "if capture:\n"
        "    with open(capture, 'w', encoding='utf-8') as handle:\n"
        "        json.dump({'argv': sys.argv[1:], 'stdin': sys.stdin.read()}, handle)\n"
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
            ),
        },
    )
    config = DaemonConfig(
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
            env={"CAPTURE_PATH": str(tmp_path / "capture.json")},
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
    assert client.failed[0][1].startswith("harness-routing:")


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
    assert client.failed[0][1].startswith("usage-limit-paused:")


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
        "[projects.demo]\n"
        f"path = {str(tmp_path)!r}\n"
        "default_harness = 'caller'\n"
        "default_model = 'fast-build'\n"
        "[actions.scope-iterate]\n"
        "runner = 'harness'\n"
        "harness = 'caller'\n"
        "model = 'fast-build'\n"
        "prompt = 'Run tests.'\n",
        encoding="utf-8",
    )

    config, actions, projects = load_config(config_path)

    assert config.routing.policy_path == policy
    assert config.routing.trusted_caller_harness == "codex"
    assert config.routing.harnesses["codex"].bin == "/opt/bin/codex"
    assert actions["scope-iterate"] == ActionConfig(
        runner="harness", harness="caller", model="fast-build", prompt="Run tests."
    )
    assert projects["demo"].default_model == "fast-build"

"""Long-running actionq coordinator using only the public ``actionctl`` API.

The daemon deliberately has no dependency on the compatibility dispatcher.  It
owns one child session at a time and records its lifecycle as coordinator
events, leaving queue mutation to ``actionctl``.
"""
from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import tomllib
import uuid
from typing import Any, Callable, Protocol


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class DaemonConfig:
    poll_interval_seconds: float = 30.0
    heartbeat_interval_seconds: float = 60.0
    graceful_shutdown_seconds: float = 30.0
    default_timeout_minutes: int = 30
    session_state_path: Path = Path("~/.local/state/actionq/sessions.json")
    pause_file: Path = Path("~/.local/state/actionq/PAUSED")
    actionctl_bin: str = "actionctl"


@dataclass(frozen=True)
class ActionConfig:
    runner: str = "fake"
    timeout_minutes: int | None = None
    fake_duration_seconds: float = 0.0


@dataclass
class SessionRecord:
    session_id: str
    runtime_session_id: str
    daemon_id: str
    action_id: int
    action_type: str
    project: str | None
    target_ref: str | None
    runner: str
    pid: int | None
    started_at: str | None
    updated_at: str


class CoordinatorClient(Protocol):
    def claim(self, worker: str, timeout_minutes: int) -> dict[str, Any] | None: ...
    def emit(self, event_type: str, *, action_id: int | None, actor: str, payload: dict[str, Any]) -> None: ...
    def complete(self, action_id: int, *, result_ref: str, actor: str) -> None: ...
    def fail(self, action_id: int, *, reason: str, actor: str) -> None: ...


class ActionctlClient:
    def __init__(self, executable: str):
        self.executable = executable

    def _run(self, *args: str, allow_empty: bool = False) -> dict[str, Any] | None:
        completed = subprocess.run(
            [self.executable, *args], text=True, capture_output=True, check=False
        )
        if allow_empty and completed.returncode == 2:
            return None
        if completed.returncode:
            detail = completed.stderr.strip() or completed.stdout.strip() or "actionctl failed"
            raise RuntimeError(detail)
        return json.loads(completed.stdout)

    def claim(self, worker: str, timeout_minutes: int) -> dict[str, Any] | None:
        return self._run("claim", "--worker", worker, "--timeout", str(timeout_minutes), allow_empty=True)

    def emit(self, event_type: str, *, action_id: int | None, actor: str, payload: dict[str, Any]) -> None:
        args = ["emit", "--type", event_type, "--actor", actor, "--payload", json.dumps(payload, sort_keys=True)]
        if action_id is not None:
            args.extend(["--action", str(action_id)])
        self._run(*args)

    def complete(self, action_id: int, *, result_ref: str, actor: str) -> None:
        self._run("complete", str(action_id), "--result", result_ref, "--actor", actor)

    def fail(self, action_id: int, *, reason: str, actor: str) -> None:
        self._run("fail", str(action_id), "--reason", reason, "--actor", actor)


def load_config(path: Path) -> tuple[DaemonConfig, dict[str, ActionConfig]]:
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    global_raw = raw.get("global", {})
    state_path = Path(global_raw.get("session_state_path", DaemonConfig.session_state_path)).expanduser()
    pause_file = Path(global_raw.get("pause_file", DaemonConfig.pause_file)).expanduser()
    config = DaemonConfig(
        poll_interval_seconds=float(global_raw.get("poll_interval_seconds", 30)),
        heartbeat_interval_seconds=float(global_raw.get("heartbeat_interval_seconds", 60)),
        graceful_shutdown_seconds=float(global_raw.get("graceful_shutdown_seconds", 30)),
        default_timeout_minutes=int(global_raw.get("default_timeout_minutes", 30)),
        session_state_path=state_path,
        pause_file=pause_file,
        actionctl_bin=str(global_raw.get("actionctl_bin", "actionctl")),
    )
    actions = {
        name: ActionConfig(
            runner=str(value.get("runner", "fake")),
            timeout_minutes=(int(value["timeout_minutes"]) if "timeout_minutes" in value else None),
            fake_duration_seconds=float(value.get("fake_duration_seconds", 0)),
        )
        for name, value in raw.get("actions", {}).items()
    }
    return config, actions


class Daemon:
    def __init__(
        self,
        config: DaemonConfig,
        actions: dict[str, ActionConfig],
        client: CoordinatorClient,
        reload_config: Callable[[], tuple[DaemonConfig, dict[str, ActionConfig]]] | None = None,
    ):
        self.config, self.actions, self.client = config, actions, client
        self.daemon_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4()}"
        self.actor = f"actionq-daemon:{self.daemon_id}"
        self._shutdown = False
        self._reload_requested = False
        self._reload_config = reload_config
        self._child: subprocess.Popen[str] | None = None

    def request_shutdown(self, *_: object) -> None:
        self._shutdown = True

    def request_reload(self, *_: object) -> None:
        """Defer SIGHUP reload until no child process is active."""
        self._reload_requested = True

    def _write_state(self, record: SessionRecord | None) -> None:
        path = self.config.session_state_path
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(asdict(record) if record else {}, sort_keys=True), encoding="utf-8")
        temporary.replace(path)

    def run_once(self) -> bool:
        if self.config.pause_file.exists():
            self.client.emit(
                "coordinator_paused",
                action_id=None,
                actor=self.actor,
                payload={"daemon_id": self.daemon_id, "pause_file": str(self.config.pause_file)},
            )
            return False
        action = self.client.claim(self.actor, self.config.default_timeout_minutes)
        if action is None:
            return False
        self._run_action(action)
        return True

    def _run_action(self, action: dict[str, Any]) -> None:
        action_id = int(action["id"])
        action_type = str(action["action_type"])
        action_config = self.actions.get(action_type)
        if action_config is None:
            self.client.fail(action_id, reason=f"no daemon config for action type {action_type}", actor=self.actor)
            return
        session_id = f"aqs:{uuid.uuid4()}"
        payload = {
            "session_id": session_id, "runtime_session_id": session_id,
            "daemon_id": self.daemon_id, "action_id": action_id,
            "action_type": action_type, "project": action.get("project"),
            "target_ref": action.get("target_ref"), "runner": action_config.runner,
        }
        self.client.emit("session.dispatch", action_id=action_id, actor=self.actor, payload=payload)
        record = SessionRecord(session_id, session_id, self.daemon_id, action_id, action_type,
                               action.get("project"), action.get("target_ref"), action_config.runner,
                               None, None, _now())
        try:
            self._child = self._start_child(action_config)
            record.pid, record.started_at, record.updated_at = self._child.pid, _now(), _now()
            self._write_state(record)
            self.client.emit("session.started", action_id=action_id, actor=self.actor,
                             payload={**payload, "pid": record.pid, "started_at": record.started_at})
            outcome, exit_code = self._wait_for_child(action_id, payload, record)
            exited = {**payload, "pid": record.pid, "outcome": outcome, "exit_code": exit_code, "exited_at": _now()}
            self.client.emit("session.exited", action_id=action_id, actor=self.actor, payload=exited)
            if outcome == "completed":
                self.client.complete(action_id, result_ref=f"session={session_id}", actor=self.actor)
            else:
                self.client.fail(action_id, reason=f"daemon session {outcome}", actor=self.actor)
        except Exception as exc:
            self.client.fail(action_id, reason=f"daemon failure: {exc}", actor=self.actor)
            raise
        finally:
            self._child = None
            self._write_state(None)

    def _start_child(self, action: ActionConfig) -> subprocess.Popen[str]:
        if action.runner not in {"fake", "fake-commit"}:
            raise RuntimeError(f"runner {action.runner!r} is not supported by daemon minimum")
        code = f"import time; time.sleep({action.fake_duration_seconds!r})"
        return subprocess.Popen([sys.executable, "-c", code], text=True, start_new_session=True)

    def _wait_for_child(self, action_id: int, payload: dict[str, Any], record: SessionRecord) -> tuple[str, int]:
        assert self._child is not None
        next_heartbeat = time.monotonic() + self.config.heartbeat_interval_seconds
        while self._child.poll() is None:
            if self._shutdown:
                self.client.emit("session.paused", action_id=action_id, actor=self.actor,
                                 payload={**payload, "pid": record.pid, "reason": "shutdown"})
                try:
                    self._child.wait(timeout=self.config.graceful_shutdown_seconds)
                except subprocess.TimeoutExpired:
                    os.killpg(self._child.pid, signal.SIGTERM)
                return "shutdown", self._child.wait()
            if time.monotonic() >= next_heartbeat:
                record.updated_at = _now()
                self._write_state(record)
                self.client.emit("session.heartbeat", action_id=action_id, actor=self.actor,
                                 payload={**payload, "pid": record.pid, "status": "running"})
                next_heartbeat = time.monotonic() + self.config.heartbeat_interval_seconds
            time.sleep(0.05)
        exit_code = self._child.returncode
        return ("completed" if exit_code == 0 else "failed"), int(exit_code)

    def run_forever(self) -> None:
        while not self._shutdown:
            if self._reload_requested and self._child is None and self._reload_config:
                self.config, self.actions = self._reload_config()
                self._reload_requested = False
            claimed = self.run_once()
            if not claimed:
                time.sleep(self.config.poll_interval_seconds)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the actionq daemon")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    config_path = args.config or Path("~/.config/actionq/config.toml").expanduser()
    if not config_path.exists() and args.config is None:
        config_path = Path("~/.config/actionq-dispatcher/config.toml").expanduser()
    config, actions = load_config(config_path)
    daemon = Daemon(config, actions, ActionctlClient(config.actionctl_bin), lambda: load_config(config_path))
    signal.signal(signal.SIGTERM, daemon.request_shutdown)
    signal.signal(signal.SIGINT, daemon.request_shutdown)
    signal.signal(signal.SIGHUP, daemon.request_reload)
    if args.once:
        daemon.run_once()
    else:
        daemon.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

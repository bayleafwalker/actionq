"""Compatibility exports for the actionq daemon services.

Configuration, clients, and the worker runtime are split into focused modules;
this module preserves the historical public import path and monkeypatch seams.
"""

from __future__ import annotations

import argparse
import signal
from pathlib import Path

from . import daemon_runtime as _runtime
from . import daemon_audit as _audit
from . import daemon_claim as _claim
from . import daemon_lifecycle as _lifecycle
from . import daemon_runner as _runner
from .daemon_config import *
from .daemon_config import _audit_refs, _is_shared_sprint_backend, _now
from .daemon_clients import *
from .daemon_runtime import Daemon as _Daemon
from .cas import _DaemonCAS


def _sync_runtime_namespace() -> None:
    for name in (
        "subprocess",
        "shutil",
        "get_adapter",
        "load_policy",
        "classify_usage_limit",
        "write_handoff",
    ):
        if name in globals():
            value = globals()[name]
            for module in (_runtime, _audit, _claim, _lifecycle, _runner):
                setattr(module, name, value)


class Daemon(_Daemon):
    """Compatibility subclass that keeps daemon module monkeypatches live."""

    def __getattribute__(self, name):
        if not name.startswith("__"):
            _sync_runtime_namespace()
        return super().__getattribute__(name)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the actionq daemon")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    config_path = args.config or Path("~/.config/actionq/config.toml").expanduser()
    if not config_path.exists() and args.config is None:
        config_path = Path("~/.config/actionq-dispatcher/config.toml").expanduser()
    config, actions, projects = load_config(config_path)
    daemon = Daemon(config, actions, ActionctlClient(
        config.actionctl_bin, runnerctl=config.runnerctl_bin,
        runner_private_key_path=config.runner_private_key_path,
        artifact_root=config.artifact_root,
    ), projects,
                    reload_config=lambda: load_config(config_path))
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


# main is intentionally defined here so python -m actionq.daemon keeps
# the established entrypoint while the implementation lives in daemon_runtime.

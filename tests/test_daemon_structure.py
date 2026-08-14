"""Structural guardrails for daemon compatibility and service boundaries."""

from actionq.daemon import ActionctlClient, Daemon, DaemonConfig, load_config
from actionq.daemon_clients import ClaimClient
from actionq.daemon_config import AuditConfig
from actionq.daemon_runtime import Daemon as RuntimeDaemon
from actionq.daemon_audit import DaemonAuditMixin
from actionq.daemon_claim import DaemonClaimMixin
from actionq.daemon_lifecycle import DaemonLifecycleMixin
from actionq.daemon_runner import DaemonRunnerMixin


def test_daemon_compatibility_surface_exports_runtime_and_clients():
    assert Daemon.__mro__[1] is RuntimeDaemon
    assert RuntimeDaemon.__mro__[1:5] == (
        DaemonLifecycleMixin,
        DaemonRunnerMixin,
        DaemonClaimMixin,
        DaemonAuditMixin,
    )
    assert DaemonConfig.__module__ == "actionq.daemon_config"
    assert AuditConfig.__module__ == "actionq.daemon_config"
    assert ActionctlClient.__module__ == "actionq.daemon_clients"
    assert ClaimClient.__module__ == "actionq.daemon_clients"
    assert load_config.__module__ == "actionq.daemon_config"


def test_daemon_methods_live_in_focused_boundaries():
    assert RuntimeDaemon.run_forever.__module__ == DaemonLifecycleMixin.__module__
    assert RuntimeDaemon._start_child.__module__ == DaemonRunnerMixin.__module__
    assert RuntimeDaemon._takeup_take.__module__ == DaemonClaimMixin.__module__
    assert RuntimeDaemon._publish_audit.__module__ == DaemonAuditMixin.__module__

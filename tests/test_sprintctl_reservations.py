from __future__ import annotations

import json
from pathlib import Path
import subprocess

from actionq.daemon import ProjectConfig, SprintctlReservationClient


def test_reservation_client_uses_credential_free_wire_commands(monkeypatch, tmp_path: Path):
    calls = []
    responses = iter((
        {"reservation": {"id": 17, "conflict": True}},
        {"reservation": {"id": 17, "state": "active"}},
        {"reservation": {"id": 17, "state": "released"}},
    ))

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args, 0, json.dumps(next(responses)), "")

    monkeypatch.setattr("actionq.daemon_clients.subprocess.run", fake_run)
    project = ProjectConfig(tmp_path, sprint_id=540, env={"SPRINTCTL_PROFILE": "devbox"})
    client = SprintctlReservationClient("sprintctl")

    assert client.reserve(
        project, item_id=1164, actor="actionq:session-1", role="execution",
        session_id="session-1", correlation_ref="actionq:session-1",
    )["id"] == 17
    assert client.touch(
        project, reservation_id=17, session_id="session-1",
        correlation_ref="actionq:session-1",
    )["state"] == "active"
    assert client.release(project, reservation_id=17, actor="actionq:session-1")["state"] == "released"

    assert calls[0][0] == [
        "sprintctl", "reservation", "reserve", "--item-id", "1164",
        "--actor", "actionq:session-1", "--role", "execution",
        "--session-id", "session-1", "--json", "--correlation-ref", "actionq:session-1",
    ]
    assert calls[1][0] == [
        "sprintctl", "reservation", "touch", "--id", "17",
        "--session-id", "session-1", "--json", "--correlation-ref", "actionq:session-1",
    ]
    assert calls[2][0] == [
        "sprintctl", "reservation", "release", "--id", "17",
        "--actor", "actionq:session-1", "--json",
    ]
    assert all(call[1]["cwd"] == tmp_path for call in calls)
    assert all(call[1]["env"]["SPRINTCTL_PROFILE"] == "devbox" for call in calls)

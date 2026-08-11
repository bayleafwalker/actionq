from __future__ import annotations

import json
from pathlib import Path

import pytest

from actionq.daemon import ActionConfig, Daemon, DaemonConfig, _DaemonCAS
from actionq_runner import FilesystemCAS, artifact_ref
from actionq_contracts import artifact_digest


class CheckingClient:
    def __init__(self, store: FilesystemCAS):
        self.store = store
        self.settled: list[dict] = []
        self.referent_checked = False
        self.lose_first_response = False

    def settle(self, action_id: int, *, result: dict, actor: str, claim_receipt: str) -> None:
        assert result["action_id"] == action_id
        value = self.store.get(result["result_ref"])
        assert artifact_ref(value) == result["result_ref"]
        assert artifact_digest(result["result_ref"]) == result["result_digest"]
        self.referent_checked = True
        self.settled.append(dict(result))
        if self.lose_first_response and len(self.settled) == 1:
            raise ConnectionError("response lost after commit")


class RecoveryClient(CheckingClient):
    def show(self, action_id: int) -> dict:
        result = self.settled[-1]
        return {
            "action": {
                "id": action_id,
                "status": "completed",
                "result_ref": result["result_ref"],
                "completed_at": "2026-08-11T00:00:00Z",
            },
            "events": [{"event_type": "action_completed", "payload": result}],
        }

    def emit(self, *_args, **_kwargs) -> None:
        pass


@pytest.mark.real_daemon_result_cas
def test_server_local_daemon_cas_rejects_missing_and_rehashed_objects(tmp_path: Path) -> None:
    root = tmp_path / "durable-cas"
    root.mkdir(mode=0o700)
    store = _DaemonCAS(root, allow_unsafe_test_root=True)
    reference = store.put(b"server-owned-result")
    assert store.get(reference) == b"server-owned-result"

    store._path(reference).unlink()
    with pytest.raises(FileNotFoundError):
        store.get(reference)

    reference = store.put(b"server-owned-result")
    store._path(reference).write_bytes(b"rehashed-bytes")
    store._path(reference).chmod(0o600)
    with pytest.raises(RuntimeError, match="corrupt"):
        store.get(reference)


def _daemon(tmp_path: Path, client: CheckingClient) -> Daemon:
    root = tmp_path / "durable-cas"
    root.mkdir(mode=0o700)
    return Daemon(
        DaemonConfig(
            artifact_root=root,
            session_state_path=tmp_path / "state.json",
            pause_file=tmp_path / "PAUSED",
        ),
        {"fake": ActionConfig(fake_duration_seconds=0.0)},
        client,
    )


@pytest.mark.parametrize("stop_reason", sorted({
    "cancelled", "claim-lost", "crash-inferred", "process-exit", "settlement-failed",
    "start-failed", "timeout", "usage-limit", "verification-failed",
}))
def test_each_bounded_failure_settlement_precedes_only_after_cas_referent_exists(
    tmp_path: Path, stop_reason: str,
) -> None:
    root = tmp_path / "durable-cas"
    root.mkdir(mode=0o700)
    store = FilesystemCAS(root, allow_unsafe_test_root=True)
    client = CheckingClient(store)
    daemon = Daemon(
        DaemonConfig(
            artifact_root=root,
            session_state_path=tmp_path / "state.json",
            pause_file=tmp_path / "PAUSED",
        ),
        {},
        client,
    )

    daemon._settle_result(
        {"id": 2124, "attempt_id": "aqs:attempt-1", "claim_attempt_id": "aqs:attempt-1"},
        claim_receipt="private-receipt", terminal_status="failed", stop_reason=stop_reason,
    )

    result = client.settled[0]
    raw = store.get(result["result_ref"])
    assert client.referent_checked
    assert artifact_ref(raw) == result["result_ref"]
    assert artifact_digest(result["result_ref"]) == result["result_digest"]
    assert json.loads(raw) == {
        "action_id": 2124,
        "attempt_id": "aqs:attempt-1",
        "contract_id": "dispatch-result/v1",
        "stop_reason": stop_reason,
        "terminal_status": "failed",
    }


def test_success_artifact_is_durable_before_settlement(tmp_path: Path) -> None:
    root = tmp_path / "durable-cas"
    root.mkdir(mode=0o700)
    store = FilesystemCAS(root, allow_unsafe_test_root=True)
    client = CheckingClient(store)
    daemon = Daemon(
        DaemonConfig(artifact_root=root), {}, client,
    )

    daemon._settle_result(
        {"id": 2124, "attempt_id": "aqs:attempt-success", "claim_attempt_id": "aqs:attempt-success"},
        claim_receipt="private-receipt", terminal_status="completed",
    )

    assert client.referent_checked
    assert client.settled[0]["stop_reason"] is None


def test_early_start_failure_writes_result_artifact_before_settlement(tmp_path: Path) -> None:
    root = tmp_path / "durable-cas"
    root.mkdir(mode=0o700)
    store = FilesystemCAS(root, allow_unsafe_test_root=True)
    client = CheckingClient(store)
    daemon = Daemon(
        DaemonConfig(
            artifact_root=root,
            session_state_path=tmp_path / "state.json",
            pause_file=tmp_path / "PAUSED",
        ),
        {},
        client,
    )

    assert daemon._run_action({
        "id": 2124,
        "action_type": "not-configured",
        "attempt_id": "aqs:attempt-start-failure",
        "claim_attempt_id": "aqs:attempt-start-failure",
        "claim_receipt": "private-receipt",
        "runner_auth_token": "private-runner-auth",
    }) is None
    assert client.settled[0]["terminal_status"] == "blocked"
    assert client.settled[0]["stop_reason"] == "start-failed"
    assert client.referent_checked


def test_response_loss_retry_and_recovery_reuse_the_same_durable_artifact(tmp_path: Path) -> None:
    root = tmp_path / "durable-cas"
    root.mkdir(mode=0o700)
    store = FilesystemCAS(root, allow_unsafe_test_root=True)
    client = CheckingClient(store)
    client.lose_first_response = True
    daemon = Daemon(DaemonConfig(artifact_root=root), {}, client)
    action = {"id": 2124, "attempt_id": "aqs:attempt-retry", "claim_attempt_id": "aqs:attempt-retry"}

    with pytest.raises(ConnectionError, match="response lost"):
        daemon._settle_result(action, claim_receipt="private-receipt", terminal_status="completed")
    daemon._settle_result(action, claim_receipt="private-receipt", terminal_status="completed")

    assert len(client.settled) == 2
    assert client.settled[0]["result_ref"] == client.settled[1]["result_ref"]
    assert client.settled[0]["result_digest"] == client.settled[1]["result_digest"]
    assert client.referent_checked


def test_publication_recovery_verifies_stored_receipt_before_settlement(tmp_path: Path) -> None:
    root = tmp_path / "durable-cas"
    root.mkdir(mode=0o700)
    store = FilesystemCAS(root, allow_unsafe_test_root=True)
    receipt = b'{"contract_id":"publication-receipt/v1","action_id":2124}'
    journal_ref = store.put(receipt)
    client = RecoveryClient(store)
    daemon = Daemon(DaemonConfig(artifact_root=root), {}, client)
    daemon._ack_publication_settlement = lambda *_args, **_kwargs: None

    daemon._complete_published(
        2124,
        claim_attempt_id="aqs:attempt-recovered",
        claim_receipt="private-receipt",
        publication={
            "action_id": 2124,
            "attempt_id": "aqs:attempt-recovered",
            "journal_ref": journal_ref,
        },
    )

    result = client.settled[0]
    assert store.get(result["result_ref"]) == receipt
    assert artifact_ref(store.get(result["result_ref"])) == result["result_ref"]
    assert artifact_digest(result["result_ref"]) == result["result_digest"]


def test_publication_recovery_response_loss_reuses_verified_digest(tmp_path: Path) -> None:
    root = tmp_path / "durable-cas"
    root.mkdir(mode=0o700)
    store = FilesystemCAS(root, allow_unsafe_test_root=True)
    journal_ref = store.put(b"publication-receipt")
    client = RecoveryClient(store)
    client.lose_first_response = True
    daemon = Daemon(DaemonConfig(artifact_root=root), {}, client)
    daemon._ack_publication_settlement = lambda *_args, **_kwargs: None

    daemon._complete_published(
        2124,
        claim_attempt_id="aqs:attempt-recovered",
        claim_receipt="private-receipt",
        publication={"action_id": 2124, "attempt_id": "aqs:attempt-recovered", "journal_ref": journal_ref},
    )

    result = client.settled[0]
    assert result["result_ref"] == journal_ref
    assert result["result_digest"] == artifact_digest(journal_ref)
    assert client.show(2124)["action"]["result_ref"] == journal_ref


@pytest.mark.real_daemon_result_cas
def test_settlement_fails_closed_without_a_valid_durable_root(tmp_path: Path) -> None:
    client = CheckingClient(FilesystemCAS(tmp_path, allow_unsafe_test_root=True))
    daemon = Daemon(DaemonConfig(artifact_root=None), {}, client)

    with pytest.raises(RuntimeError, match="provisioned durable artifact_root"):
        daemon._settle_result(
            {"id": 2124, "attempt_id": "aqs:attempt-no-root", "claim_attempt_id": "aqs:attempt-no-root"},
            claim_receipt="private-receipt", terminal_status="completed",
        )
    assert not client.settled

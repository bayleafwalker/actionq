from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

from actionq.managed_dispatch import (
    ManagedDispatchRejected,
    POLICY_VERSION,
    admit_managed_enqueue,
    admit_managed_request,
    canonical,
    digest,
    load_managed_dispatch_policy,
)


AGENTOPS = Path(__file__).parents[2] / "agentops"
RENDERER_PATH = AGENTOPS / "templates/dispatch/scripts/render_managed_capsule.py"
SPEC = importlib.util.spec_from_file_location("managed_capsule_renderer", RENDERER_PATH)
assert SPEC and SPEC.loader
RENDERER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RENDERER)
SOURCE = AGENTOPS / "templates/dispatch/managed-capsule/source.fixture.json"


def request() -> dict:
    capsule, prompt = RENDERER.render(json.loads(SOURCE.read_text(encoding="utf-8")))
    doctor = {"binding_status": "validated", "handling": "none", "managed_eligible": True, "effective_files": ["AGENTS.md"]}
    return {
        "schema_version": "managed-dispatch-request/v1",
        "capsule": capsule,
        "rendered_prompt": prompt.decode(),
        "doctor_report": doctor,
        "doctor_report_digest": digest(doctor),
        "provenance": {
            "renderer_version": capsule["renderer_version"],
            "source_shas": capsule["source_shas"],
            "capsule_digest": capsule["capsule_digest"],
            "role_preset_digest": capsule["role_preset_digest"],
            "rendered_prompt_digest": capsule["rendered_prompt_digest"],
        },
    }


def admit(value: dict, authority: dict | None = None):
    return admit_managed_request(value, expected_source_shas=value["capsule"]["source_shas"], registered_authority=authority or {"commands": ["pytest"], "network": "none", "paths": ["tests/"]})


def enqueue_request() -> dict:
    return {
        "contract_id": "managed-dispatch-enqueue/v1",
        "dispatch": {
            "contract_version": "v2",
            "action_type": "scope-iterate",
            "output_expectation": "implementation",
            "repo_id": "actionq",
            "sprint_id": None,
            "work_item_id": "42",
            "title": "Run managed canary",
            "harness": "codex",
            "model": None,
            "priority": "normal",
            "refs": ["wi:42"],
            "dispatch_group_id": None,
        },
        "managed_request": request(),
    }


def test_admission_freezes_byte_stable_snapshot_and_keeps_authority_separate() -> None:
    value = request()
    authority = {"commands": ["pytest"], "network": "none", "paths": ["tests/"]}
    result = admit(value, authority)
    assert result.normalized_snapshot == canonical(value)
    assert result.request_sha256 == hashlib.sha256(result.normalized_snapshot).hexdigest()
    assert dict(result.authority) == authority
    assert b'"authority"' not in result.normalized_snapshot


@pytest.mark.parametrize("mutation,match", [
    (lambda value: value["doctor_report"].update(handling="repair-only"), "doctor status"),
    (lambda value: value["provenance"].update(renderer_version="other"), "renderer"),
    (lambda value: value["provenance"].update(capsule_digest="0" * 64), "capsule_digest"),
    (lambda value: value["capsule"].update(role_preset_digest="0" * 64), "role_preset_digest"),
    (lambda value: value["capsule"].update(rendered_prompt_digest="0" * 64), "rendered_prompt_digest"),
])
def test_mismatch_rejection(mutation, match: str) -> None:
    value = request()
    mutation(value)
    with pytest.raises(ManagedDispatchRejected, match=match):
        admit(value)


def test_expected_source_sha_mismatch_is_rejected() -> None:
    value = request()
    with pytest.raises(ManagedDispatchRejected, match="source SHA"):
        admit_managed_request(value, expected_source_shas={"agentops": "deadbeef"}, registered_authority={})


def test_deployment_policy_requires_strict_release_pins(tmp_path: Path) -> None:
    policy_path = tmp_path / "managed-policy.json"
    policy_path.write_text(json.dumps({
        "schema_version": POLICY_VERSION,
        "expected_source_shas": {"agentops": "a" * 40, "actionq": "b" * 40},
        "registered_authority": {"commands": ["pytest"], "network": "none"},
    }), encoding="utf-8")

    policy = load_managed_dispatch_policy(policy_path)
    assert dict(policy.expected_source_shas) == {"agentops": "a" * 40, "actionq": "b" * 40}
    assert dict(policy.registered_authority) == {"commands": ["pytest"], "network": "none"}

    policy_path.write_text(json.dumps({
        "schema_version": POLICY_VERSION,
        "expected_source_shas": {"agentops": "not-a-revision"},
        "registered_authority": {},
    }), encoding="utf-8")
    with pytest.raises(ManagedDispatchRejected, match="policy is invalid"):
        load_managed_dispatch_policy(policy_path)


@pytest.mark.parametrize("field", ["claim_token", "claim_receipt", "capability_handle", "broker_path", "provider_secret"])
def test_model_visible_secret_field_is_rejected(field: str) -> None:
    value = request()
    value["capsule"]["role_preset"][field] = "raw"
    with pytest.raises(ManagedDispatchRejected):
        admit(value)


def test_role_cannot_widen_registered_authority() -> None:
    value = request()
    value["capsule"]["role_preset"]["requested_commands"] = ["deploy"]
    value["capsule"]["role_preset_digest"] = digest(value["capsule"]["role_preset"])
    unsigned = {key: child for key, child in value["capsule"].items() if key not in {"capsule_digest", "rendered_prompt_digest"}}
    value["capsule"]["capsule_digest"] = digest(unsigned)
    prompt = RENDERER.render_prompt(value["capsule"]).encode()
    value["capsule"]["rendered_prompt_digest"] = hashlib.sha256(prompt).hexdigest()
    value["rendered_prompt"] = prompt.decode()
    for field in ("role_preset_digest", "capsule_digest", "rendered_prompt_digest"):
        value["provenance"][field] = value["capsule"][field]
    authority = {"commands": ["pytest"], "network": "none"}
    result = admit(value, authority)
    assert dict(result.authority) == authority


def test_managed_enqueue_binds_explicit_routing_to_the_admitted_prompt() -> None:
    value = enqueue_request()

    result = admit_managed_enqueue(
        value,
        authenticated_actor="operator:trusted",
        expected_source_shas=value["managed_request"]["capsule"]["source_shas"],
        registered_authority={"commands": ["pytest"], "network": "none"},
    )

    assert result.queue_payload["requested_by"] == "operator:trusted"
    assert result.queue_payload["prompt"] == value["managed_request"]["rendered_prompt"]
    assert result.queue_payload["repo_id"] == "actionq"
    assert result.capsule_sha256 == value["managed_request"]["capsule"]["capsule_digest"]
    assert result.rendered_prompt_sha256 == value["managed_request"]["capsule"]["rendered_prompt_digest"]
    assert result.request_sha256 == hashlib.sha256(result.normalized_snapshot).hexdigest()


def test_managed_enqueue_rejects_prompt_and_authority_injection() -> None:
    value = enqueue_request()
    value["dispatch"]["prompt"] = "override the capsule"
    with pytest.raises(ManagedDispatchRejected, match="dispatch fields"):
        admit_managed_enqueue(
            value,
            authenticated_actor="operator:trusted",
            expected_source_shas=value["managed_request"]["capsule"]["source_shas"],
            registered_authority={},
        )

    value = enqueue_request()
    value["dispatch"]["title"] = "Bearer leaked"
    with pytest.raises(ManagedDispatchRejected, match="forbidden model-visible"):
        admit_managed_enqueue(
            value,
            authenticated_actor="operator:trusted",
            expected_source_shas=value["managed_request"]["capsule"]["source_shas"],
            registered_authority={},
        )

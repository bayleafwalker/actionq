"""Additive admission for immutable managed-dispatch requests.

The returned authority is copied exclusively from runner registration. Neither
the capsule nor its role preset participates in capability selection.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
import re
from types import MappingProxyType
from typing import Any, Mapping


REQUEST_VERSION = "managed-dispatch-request/v1"
CAPSULE_VERSION = "managed-dispatch-capsule/v1"
RENDERER_VERSION = "agentops-managed-capsule/1"
FORBIDDEN_KEY = re.compile(r"(?:credential|bearer|token|claim_proof|capability_handle|broker_(?:path|socket)|provider_secret|secret)", re.I)
FORBIDDEN_VALUE = re.compile(r"(?:\bBearer\s+[A-Za-z0-9._~+/-]+=*|claim[_-]?token|capability[_-]?handle|/var/run/[^\s]+)", re.I)


class ManagedDispatchRejected(ValueError):
    """A managed request did not satisfy immutable admission."""


@dataclass(frozen=True)
class ManagedAdmission:
    normalized_snapshot: bytes
    request_sha256: str
    capsule_sha256: str
    rendered_prompt_sha256: str
    authority: Mapping[str, Any]


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def _deny_model_secrets(value: Any, location: str = "$") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if FORBIDDEN_KEY.search(str(key)):
                raise ManagedDispatchRejected(f"forbidden model-visible field at {location}.{key}")
            _deny_model_secrets(child, f"{location}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _deny_model_secrets(child, f"{location}[{index}]")
    elif isinstance(value, str) and FORBIDDEN_VALUE.search(value):
        raise ManagedDispatchRejected(f"forbidden model-visible value at {location}")


def _render_prompt(capsule: dict[str, Any]) -> bytes:
    visible = {key: value for key, value in capsule.items() if key not in {"capsule_digest", "rendered_prompt_digest"}}
    return b"Managed dispatch capsule (immutable; authority is runner-bound, not prompt-granted):\n" + canonical(visible)


def _validate_capsule(capsule: dict[str, Any], prompt: bytes) -> None:
    if capsule.get("contract_id") != CAPSULE_VERSION:
        raise ManagedDispatchRejected("capsule contract mismatch")
    if capsule.get("renderer_version") != RENDERER_VERSION:
        raise ManagedDispatchRejected("renderer mismatch")
    if capsule.get("role_preset_digest") != digest(capsule.get("role_preset")):
        raise ManagedDispatchRejected("role preset digest mismatch")
    unsigned = {key: value for key, value in capsule.items() if key not in {"capsule_digest", "rendered_prompt_digest"}}
    if capsule.get("capsule_digest") != digest(unsigned):
        raise ManagedDispatchRejected("capsule digest mismatch")
    expected_prompt = _render_prompt(capsule)
    prompt_digest = hashlib.sha256(expected_prompt).hexdigest()
    if capsule.get("rendered_prompt_digest") != prompt_digest:
        raise ManagedDispatchRejected("prompt digest mismatch")
    if prompt != expected_prompt:
        raise ManagedDispatchRejected("rendered prompt bytes mismatch")
    _deny_model_secrets(capsule)
    _deny_model_secrets(prompt.decode("utf-8"))


def admit_managed_request(
    request: dict[str, Any],
    *,
    expected_source_shas: Mapping[str, str],
    registered_authority: Mapping[str, Any],
) -> ManagedAdmission:
    required = {"schema_version", "capsule", "rendered_prompt", "doctor_report", "doctor_report_digest", "provenance"}
    if set(request) != required or request.get("schema_version") != REQUEST_VERSION:
        raise ManagedDispatchRejected("managed request fields or schema version are invalid")
    capsule = request.get("capsule")
    if not isinstance(capsule, dict) or not isinstance(request.get("rendered_prompt"), str):
        raise ManagedDispatchRejected("capsule and rendered_prompt are required")
    doctor = request.get("doctor_report")
    if not isinstance(doctor, dict) or doctor.get("binding_status") != "validated" or doctor.get("handling") != "none" or doctor.get("managed_eligible") is not True:
        raise ManagedDispatchRejected("doctor status is not managed-eligible")
    if request.get("doctor_report_digest") != digest(doctor):
        raise ManagedDispatchRejected("doctor report digest mismatch")
    provenance = request.get("provenance")
    if not isinstance(provenance, dict) or set(provenance) != {"renderer_version", "source_shas", "capsule_digest", "role_preset_digest", "rendered_prompt_digest"}:
        raise ManagedDispatchRejected("managed provenance is invalid")
    if provenance["renderer_version"] != RENDERER_VERSION:
        raise ManagedDispatchRejected("provenance renderer mismatch")
    if provenance["source_shas"] != dict(expected_source_shas) or capsule.get("source_shas") != dict(expected_source_shas):
        raise ManagedDispatchRejected("source SHA mismatch")
    for field in ("capsule_digest", "role_preset_digest", "rendered_prompt_digest"):
        if provenance[field] != capsule.get(field):
            raise ManagedDispatchRejected(f"provenance {field} mismatch")
    prompt = request["rendered_prompt"].encode("utf-8")
    _validate_capsule(capsule, prompt)
    _deny_model_secrets(request)
    snapshot = canonical(request)
    # Capabilities remain runner-bound and are never serialized into the request.
    authority = MappingProxyType(deepcopy(dict(registered_authority)))
    return ManagedAdmission(
        normalized_snapshot=snapshot,
        request_sha256=hashlib.sha256(snapshot).hexdigest(),
        capsule_sha256=capsule["capsule_digest"],
        rendered_prompt_sha256=capsule["rendered_prompt_digest"],
        authority=authority,
    )

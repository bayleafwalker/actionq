#!/usr/bin/env python3
"""Render and verify deterministic, prompt-visible managed dispatch capsules.

Vendored from agentops at commit 12b8f2953c8b05924fd1f38642e5b8579d44a173,
where it lived at templates/dispatch/scripts/render_managed_capsule.py.
That agentops copy (and the templates/dispatch/managed-capsule/ tree it
rendered from) is retired as part of S2 item 6 PR-E; this copy is now the
source of truth for actionq's managed-dispatch tests.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
from typing import Any


CONTRACT_ID = "managed-dispatch-capsule/v1"
SOURCE_ID = "managed-dispatch-capsule-source/v1"
RENDERER_VERSION = "agentops-managed-capsule/1"
FORBIDDEN_KEY = re.compile(r"(?:credential|bearer|token|claim_(?:proof|receipt)|capability_handle|broker_(?:path|socket)|provider_secret|secret)", re.I)
FORBIDDEN_VALUE = re.compile(r"(?:\bBearer\s+[A-Za-z0-9._~+/-]+=*|claim[_-]?token|capability[_-]?handle|/var/run/[^\s]+)", re.I)


class CapsuleError(ValueError):
    pass


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def _deny_secrets(value: Any, location: str = "$") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if FORBIDDEN_KEY.search(str(key)):
                raise CapsuleError(f"forbidden model-visible field at {location}.{key}")
            _deny_secrets(child, f"{location}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _deny_secrets(child, f"{location}[{index}]")
    elif isinstance(value, str) and FORBIDDEN_VALUE.search(value):
        raise CapsuleError(f"forbidden model-visible value at {location}")


def _require_source(source: dict[str, Any]) -> None:
    required = {"schema_version", "intent", "role_preset", "instruction_sources", "governing_refs", "dependency_context", "acceptance_ids", "source_shas", "artifacts"}
    if set(source) != required or source.get("schema_version") != SOURCE_ID:
        raise CapsuleError("source fields or schema_version are invalid")
    if not isinstance(source["intent"], str) or not source["intent"].strip():
        raise CapsuleError("intent is required")
    if not isinstance(source["role_preset"], dict):
        raise CapsuleError("role_preset must be an object")
    for field in ("instruction_sources", "governing_refs", "dependency_context", "acceptance_ids", "artifacts"):
        if not isinstance(source[field], list):
            raise CapsuleError(f"{field} must be an array")
    if not isinstance(source["source_shas"], dict):
        raise CapsuleError("source_shas must be an object")
    for item in source["dependency_context"]:
        if not isinstance(item, dict) or not item.get("selection_reason"):
            raise CapsuleError("bounded dependency context requires selection_reason")
    _deny_secrets(source)


def render_prompt(capsule: dict[str, Any]) -> str:
    visible = {key: value for key, value in capsule.items() if key not in {"capsule_digest", "rendered_prompt_digest"}}
    return "Managed dispatch capsule (immutable; authority is runner-bound, not prompt-granted):\n" + canonical(visible).decode()


def render(source: dict[str, Any]) -> tuple[dict[str, Any], bytes]:
    _require_source(source)
    capsule = {
        "contract_id": CONTRACT_ID,
        "renderer_version": RENDERER_VERSION,
        "intent": source["intent"],
        "role_preset": source["role_preset"],
        "role_preset_digest": digest(source["role_preset"]),
        "instruction_sources": source["instruction_sources"],
        "governing_refs": source["governing_refs"],
        "dependency_context": source["dependency_context"],
        "acceptance_ids": source["acceptance_ids"],
        "source_shas": source["source_shas"],
        "artifacts": source["artifacts"],
    }
    capsule["capsule_digest"] = digest(capsule)
    prompt = render_prompt(capsule).encode()
    capsule["rendered_prompt_digest"] = hashlib.sha256(prompt).hexdigest()
    _deny_secrets(capsule)
    return capsule, prompt


def verify(capsule: dict[str, Any], prompt: bytes | None = None) -> None:
    _deny_secrets(capsule)
    if capsule.get("contract_id") != CONTRACT_ID or capsule.get("renderer_version") != RENDERER_VERSION:
        raise CapsuleError("unsupported capsule contract or renderer")
    unsigned = {key: value for key, value in capsule.items() if key not in {"capsule_digest", "rendered_prompt_digest"}}
    if capsule.get("role_preset_digest") != digest(capsule.get("role_preset")):
        raise CapsuleError("role preset digest mismatch")
    if capsule.get("capsule_digest") != digest(unsigned):
        raise CapsuleError("capsule digest mismatch")
    expected_prompt = render_prompt(capsule).encode()
    if capsule.get("rendered_prompt_digest") != hashlib.sha256(expected_prompt).hexdigest():
        raise CapsuleError("rendered prompt digest mismatch")
    if prompt is not None and prompt != expected_prompt:
        raise CapsuleError("rendered prompt bytes mismatch")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    render_cmd = sub.add_parser("render")
    render_cmd.add_argument("--input", type=Path, required=True)
    render_cmd.add_argument("--capsule-out", type=Path, required=True)
    render_cmd.add_argument("--prompt-out", type=Path, required=True)
    verify_cmd = sub.add_parser("verify")
    verify_cmd.add_argument("--capsule", type=Path, required=True)
    verify_cmd.add_argument("--prompt", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "render":
            capsule, prompt = render(json.loads(args.input.read_text(encoding="utf-8")))
            args.capsule_out.write_bytes(canonical(capsule))
            args.prompt_out.write_bytes(prompt)
        else:
            verify(json.loads(args.capsule.read_text(encoding="utf-8")), args.prompt.read_bytes() if args.prompt else None)
    except (OSError, UnicodeError, json.JSONDecodeError, CapsuleError) as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""One sealed task, two ACP backends, one adapter, one acceptance path."""
import json, subprocess, sys, os, tempfile, pathlib
sys.path.insert(0, "/projects/dev/actionq/packages/actionq-runner")

from actionq_runner.acp import AcpExecutionAdapter
from actionq_runner.execution_contract import (
    ExecutionEnvelope, ExecutionInvariants, ContextPolicy,
)

TASK = "Create a file named result.txt whose only content is the word OK. Then stop."
ACCEPTANCE = "result.txt"

BACKENDS = {
    "opencode": {
        "command": ["opencode", "acp"],
        "model": "local3090/worker-fast",
        "mode": "build",
    },
    "codex": {
        "command": ["npx", "-y", "@agentclientprotocol/codex-acp@1.4.0"],
        "model": "gpt-5.6-sol[low]",
        "mode": "agent",
    },
}

def make_repo() -> pathlib.Path:
    root = pathlib.Path(tempfile.mkdtemp(prefix="fungibility-"))
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    (root / "README.md").write_text("sealed task workspace\n")
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)
    subprocess.run(["git","-c","user.email=t@t","-c","user.name=t","commit","-qm","init"],
                   cwd=root, check=True)
    return root

def run(name: str, spec: dict) -> dict:
    root = make_repo()
    envelope = ExecutionEnvelope(
        execution_id="E-fungibility-1",
        model=spec["model"],
        instruction=TASK,
        invariants=ExecutionInvariants(
            root=str(root), revision="HEAD",
            permitted_paths=("result.txt",),
            acceptance_target=ACCEPTANCE,
        ),
        context_policy=ContextPolicy(hot_material=(TASK,)),
        mode=spec["mode"],
    )
    out = {"backend": name, "root": str(root)}
    adapter = AcpExecutionAdapter(
        spec["command"], agent=name,
        permission_resolver=lambda req: __import__(
            "actionq_runner.execution_contract", fromlist=["PermissionDecision"]
        ).PermissionDecision.ALLOW,
        enforce_invariants=True,
    )
    try:
        adapter.open(envelope)
        outcome = adapter.prompt(timeout=600.0)
        out["stop_reason"] = outcome.stop_reason
        out["assurance"] = {k: v.value for k, v in adapter.assurance().items()}
        out["tools"] = [
            {"kind": c.kind, "status": c.status.value if c.status else None,
             "permission": c.permission_outcome}
            for c in adapter.tool_calls
        ]
        out["protocol_faults"] = list(adapter.protocol_faults)
        report = adapter.verify_completion()
        out["acceptance"] = {c.name: c.passed for c in report.checks}
        out["accepted"] = report.ok
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        adapter.close()
    target = pathlib.Path(out["root"]) / ACCEPTANCE
    out["artifact_present"] = target.exists()
    out["artifact"] = target.read_text().strip() if target.exists() else None
    return out

results = [run(name, spec) for name, spec in BACKENDS.items()]
print(json.dumps(results, indent=2))
pathlib.Path("fungibility-result.json").write_text(json.dumps(results, indent=2) + "\n")

"""Machine-checked execution invariants, verified around the harness.

Protocol-neutral: nothing here knows what ACP is. These checks run *outside* whatever
agent is executing, before dispatch and after completion, because the alternative --
stating the invariant inside a prompt and trusting the report -- is what produced the
failures this module exists to prevent.

    Make invalid states unrepresentable or loudly observable. Never rely on noticing
    that something silently did not happen.

A harness that can be pointed at the wrong tree by an inherited environment variable will
not be saved by an instruction telling it not to be.

The changed-path logic is deliberately shared with ``executor``: the released portable
envelope already enforces exactly this rule against a frozen source commit, and a second
implementation of the same check is a second thing to get wrong.
"""
from __future__ import annotations

import fnmatch
import hashlib
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from .execution_contract import ExecutionInvariants
from .executor import _changed_paths


class BoundaryViolation(RuntimeError):
    """An execution invariant did not hold. Never a warning."""

    def __init__(self, report: "BoundaryReport") -> None:
        failures = "; ".join(
            f"{c.name}: expected {c.expected!r}, observed {c.observed!r}"
            for c in report.violations
        )
        super().__init__(f"execution boundary violated -- {failures}")
        self.report = report


@dataclass(frozen=True)
class BoundaryCheck:
    name: str
    passed: bool
    expected: str | None = None
    observed: str | None = None
    detail: str | None = None


@dataclass(frozen=True)
class BoundaryReport:
    phase: str
    checks: tuple[BoundaryCheck, ...] = ()

    @property
    def violations(self) -> tuple[BoundaryCheck, ...]:
        return tuple(c for c in self.checks if not c.passed)

    @property
    def ok(self) -> bool:
        return not self.violations

    def raise_for_violations(self) -> "BoundaryReport":
        if not self.ok:
            raise BoundaryViolation(self)
        return self


def _git(root: Path, *args: str) -> tuple[int, str]:
    completed = subprocess.run(
        ["git", *args], cwd=root, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )
    return completed.returncode, completed.stdout.strip()


def resolve_revision(root: str | Path) -> str | None:
    code, head = _git(Path(root), "rev-parse", "HEAD")
    return head if code == 0 and head else None


def verify_before_dispatch(invariants: ExecutionInvariants) -> BoundaryReport:
    """Check the tree is the tree the envelope named, before any agent runs.

    ``revision`` may be the literal ``HEAD``, meaning "whatever this worktree is on, and
    record it" -- useful for exploratory work. Any other value is a pin and must match.
    """
    root = Path(invariants.root)
    checks: list[BoundaryCheck] = []

    resolved = root.resolve() if root.exists() else None
    checks.append(BoundaryCheck(
        name="root.exists",
        passed=resolved is not None and resolved.is_dir(),
        expected=str(root),
        observed=str(resolved) if resolved else "missing",
    ))
    if resolved is None or not resolved.is_dir():
        return BoundaryReport(phase="before_dispatch", checks=tuple(checks))

    code, top = _git(resolved, "rev-parse", "--show-toplevel")
    is_worktree = code == 0 and bool(top)
    checks.append(BoundaryCheck(
        name="root.is_worktree",
        passed=is_worktree,
        expected="a git worktree",
        observed=top if is_worktree else "not a git worktree",
    ))

    if is_worktree:
        # The root must be the worktree top, not a subdirectory of it. A subdirectory
        # would make the changed-path allowlist silently relative to the wrong base.
        checks.append(BoundaryCheck(
            name="root.is_worktree_top",
            passed=Path(top).resolve() == resolved,
            expected=str(resolved),
            observed=str(Path(top).resolve()),
        ))

        head = resolve_revision(resolved)
        if invariants.revision == "HEAD":
            checks.append(BoundaryCheck(
                name="revision.recorded",
                passed=head is not None,
                expected="a resolvable HEAD",
                observed=head or "unresolvable",
                detail="revision was not pinned; HEAD recorded for the post-check",
            ))
        else:
            checks.append(BoundaryCheck(
                name="revision.pinned",
                passed=head == invariants.revision,
                expected=invariants.revision,
                observed=head or "unresolvable",
            ))

    return BoundaryReport(phase="before_dispatch", checks=tuple(checks))


def _fingerprint(root: Path, relative: str) -> str:
    """Identify a path's *state*, enough to tell "unchanged" from "changed again".

    Content plus presence and type. Provenance is not needed here -- the only question
    the boundary asks is whether the final tree differs from the baseline at this path.
    """
    target = root / relative
    if target.is_symlink():
        return "symlink:" + str(Path(target).readlink())
    if not target.exists():
        return "absent"
    if target.is_dir():
        return "dir"
    try:
        digest = hashlib.sha256(target.read_bytes()).hexdigest()
    except OSError:
        return "unreadable"
    return "file:" + digest


@dataclass(frozen=True)
class BaselineState:
    """Pre-existing dirt, recorded by state rather than by name.

    A bare set of paths cannot distinguish *inherited* dirt from a path the execution
    dirtied **further**: subtracting the set removes the second case along with the first,
    and the boundary reports clean while something really did happen. Recording a
    fingerprint per already-dirty path closes that.
    """

    fingerprints: Mapping[str, str] = field(default_factory=dict)

    def __contains__(self, relative: str) -> bool:
        return relative in self.fingerprints

    def unchanged(self, root: Path, relative: str) -> bool:
        recorded = self.fingerprints.get(relative)
        return recorded is not None and recorded == _fingerprint(root, relative)


def snapshot_baseline(root: str | Path) -> BaselineState:
    """Record pre-existing dirt *and its state*, before the execution runs."""
    base = Path(root)
    dirty = _changed_paths(base)
    return BaselineState(fingerprints={p: _fingerprint(base, p) for p in dirty})


def _attributable_changes(root: Path, baseline: BaselineState) -> set[str]:
    """Paths this execution is answerable for.

    Four cases, only the second of which is inherited:

        newly dirty                          -> changed by the execution
        previously dirty, same fingerprint   -> inherited dirt, not attributable
        previously dirty, different state    -> changed by the execution
        previously dirty, no longer dirty    -> reverted, changed by the execution
    """
    now = set(_changed_paths(root))
    changed = {p for p in now if p not in baseline or not baseline.unchanged(root, p)}
    changed |= {p for p in baseline.fingerprints if p not in now}
    return changed


def verify_after_completion(
    invariants: ExecutionInvariants,
    *,
    baseline: BaselineState | None = None,
    expected_revision: str | None = None,
) -> BoundaryReport:
    """Check the agent stayed inside what the envelope permitted.

    An empty ``permitted_paths`` means no modification was authorised, and any change is a
    violation. That is the fail-closed reading, and it matches the frozen allowlist the
    portable runner already enforces.
    """
    root = Path(invariants.root)
    checks: list[BoundaryCheck] = []

    if not root.is_dir():
        return BoundaryReport(phase="after_completion", checks=(
            BoundaryCheck("root.exists", False, str(root), "missing"),
        ))

    pin = expected_revision or (
        invariants.revision if invariants.revision != "HEAD" else None
    )
    if pin is not None:
        head = resolve_revision(root)
        checks.append(BoundaryCheck(
            name="revision.unchanged",
            passed=head == pin,
            expected=pin,
            observed=head or "unresolvable",
            detail="the harness committed or moved HEAD" if head != pin else None,
        ))

    changed = _attributable_changes(root, baseline or BaselineState())
    disallowed = sorted(
        path for path in changed
        if not any(fnmatch.fnmatchcase(path, pattern) for pattern in invariants.permitted_paths)
    )
    checks.append(BoundaryCheck(
        name="paths.permitted",
        passed=not disallowed,
        expected=str(list(invariants.permitted_paths)),
        observed=str(disallowed) if disallowed else "no disallowed changes",
        detail=None if not disallowed else "changes outside the permitted allowlist",
    ))

    if invariants.acceptance_target:
        target = root / invariants.acceptance_target
        checks.append(BoundaryCheck(
            name="acceptance.target_present",
            passed=target.exists(),
            expected=invariants.acceptance_target,
            observed="present" if target.exists() else "missing",
        ))

    return BoundaryReport(phase="after_completion", checks=tuple(checks))

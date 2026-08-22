"""Every normative claim in an opted-in plan document names a falsifying test.

Measurement track item 2.  The four W3 review rounds produced two defects that
no amount of test-writing would have caught, because the tests encoded the
belief being tested:

* "mutable facts last" was documented as load-bearing and was not -- a reviewer
  disproved it by inverting the order and observing identical writes;
* "putting expected_revision in the idempotency key makes a rejected fact
  recoverable" was claimed in a commit message and a PR comment. The test for it
  passed, and passed *for the right reason* -- but it exercised the case where
  the live revision had moved, which is narrower than the claim the prose made.

So presence of a matching test is necessary and not sufficient, and this module
checks both dimensions:

1. **Coverage** -- every inline ``<!-- claim: id -->`` marker is accounted for
   in the document's ``falsifiers`` block, and vice versa.  A claim may be
   declared an explicit gap, but only with a reason, and the coverage ratio is
   pinned below so that lowering it is a visible edit rather than a silent
   drift.
2. **Scope** -- the falsifier records the scope the claim is limited to, and
   that text must appear in the named test's own docstring.  Widening a claim
   therefore fails here until someone edits the test's docstring, which is the
   moment to ask whether the test still proves what the claim now says.

Neither check can make a claim true.  What they do is stop a claim from being
broadened silently, which is exactly how both W3 defects reached a PR comment.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import pytest


ROOT = Path(__file__).parents[1]
DOC_ROOTS = (ROOT / "docs/plans", ROOT / "docs/contracts")
CLAIM_MARKER = re.compile(r"<!--\s*claim:\s*([a-z0-9][a-z0-9-]*)\s*-->")
FALSIFIER_BLOCK = re.compile(r"```falsifiers\n(.*?)\n```", re.DOTALL)
TEST_REFERENCE = re.compile(r"^(tests/[A-Za-z0-9_]+\.py)::([A-Za-z0-9_]+)$")
REQUIRED_FIELDS = {"id", "claim", "scope"}

# Floor no document may declare below, whatever kind it is.
ABSOLUTE_FLOOR = 0.0


def _documents() -> list[tuple[Path, str]]:
    """Documents that opt in, discovered by *either* signal.

    Keying discovery on the falsifiers block alone would make a document that
    carries claim markers and no block invisible to every check below -- the one
    shape that most needs catching, since it is what a half-finished opt-in
    looks like.
    """
    found: list[tuple[Path, str]] = []
    for root in DOC_ROOTS:
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*.md")):
            text = path.read_text(encoding="utf-8")
            if FALSIFIER_BLOCK.search(text) or CLAIM_MARKER.search(text):
                found.append((path, text))
    return found


def _block(text: str, path: Path) -> dict:
    """The document's falsifier declaration.

    An object, not a bare list, because each document declares its own coverage
    target. A contract document asserting properties of code at rest can hold a
    high one; a scope document making ordering and boundary claims about a
    change set that does not exist yet legitimately cannot, and forcing one
    number on both would either block the scope document or set the number so
    low it never bites the contract. The target is pinned in the document, so
    lowering it is a visible edit in review rather than a silent drift.
    """
    blocks = FALSIFIER_BLOCK.findall(text)
    assert len(blocks) == 1, (
        f"{path}: expected exactly one falsifiers block, found {len(blocks)}"
        + ("; the document carries claim markers, so it has opted in and owes one"
           if not blocks and CLAIM_MARKER.search(text) else "")
    )
    try:
        parsed = json.loads(blocks[0])
    except ValueError as malformed:  # pragma: no cover - failure path is the point
        pytest.fail(f"{path}: falsifiers block is not valid JSON: {malformed}")
    assert isinstance(parsed, dict), f"{path}: falsifiers block must be an object"
    target = parsed.get("minimum_coverage")
    assert isinstance(target, (int, float)) and ABSOLUTE_FLOOR <= target <= 1, (
        f"{path}: falsifiers block must declare a minimum_coverage between {ABSOLUTE_FLOOR} and 1"
    )
    entries = parsed.get("falsifiers")
    assert isinstance(entries, list) and entries, f"{path}: falsifiers must be a non-empty list"
    return parsed


def _falsifiers(text: str, path: Path) -> list[dict]:
    return _block(text, path)["falsifiers"]


def _test_functions(relative: str) -> set[str]:
    path = ROOT / relative
    assert path.is_file(), f"falsifier names a test file that does not exist: {relative}"
    return {
        node.name
        for node in ast.parse(path.read_text(encoding="utf-8")).body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _docstring(relative: str, name: str) -> str:
    tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return ast.get_docstring(node) or ""
    return ""


def _flow(value: str) -> str:
    """Whitespace-insensitive, so a claim wrapping differently still matches."""
    return " ".join(value.split())


def test_at_least_one_document_opts_in() -> None:
    """The checker is worthless if nothing is registered; fail rather than pass
    vacuously when every document has dropped its falsifiers block."""
    assert _documents(), "no plan document carries a falsifiers block"


def test_every_marked_claim_is_accounted_for_and_every_falsifier_is_marked() -> None:
    for path, text in _documents():
        marked = CLAIM_MARKER.findall(text)
        assert len(marked) == len(set(marked)), f"{path}: duplicate claim markers"
        declared = [entry["id"] for entry in _falsifiers(text, path)]
        assert len(declared) == len(set(declared)), f"{path}: duplicate falsifier ids"
        assert set(marked) == set(declared), (
            f"{path}: claims and falsifiers disagree; "
            f"unfalsified={sorted(set(marked) - set(declared))} "
            f"unmarked={sorted(set(declared) - set(marked))}"
        )


def test_every_falsifier_is_well_formed_and_resolves_to_a_real_test() -> None:
    for path, text in _documents():
        for entry in _falsifiers(text, path):
            missing = REQUIRED_FIELDS - set(entry)
            assert not missing, f"{path}: falsifier {entry.get('id')!r} is missing {sorted(missing)}"
            for field in REQUIRED_FIELDS:
                assert isinstance(entry[field], str) and entry[field].strip(), (
                    f"{path}: falsifier {entry['id']!r} has an empty {field}"
                )
            reference = entry.get("test")
            if reference is None:
                # An explicit, reasoned gap. Visible and counted, which is the
                # whole point -- an absent claim is invisible, a declared gap is
                # a decision someone can disagree with.
                assert isinstance(entry.get("gap"), str) and entry["gap"].strip(), (
                    f"{path}: falsifier {entry['id']!r} has no test and no gap reason"
                )
                continue
            matched = TEST_REFERENCE.fullmatch(reference)
            assert matched, f"{path}: falsifier {entry['id']!r} has a malformed test reference {reference!r}"
            relative, name = matched.groups()
            assert name in _test_functions(relative), (
                f"{path}: falsifier {entry['id']!r} names {name}, which {relative} does not define"
            )


def test_every_falsifier_scope_is_restated_by_the_test_it_names() -> None:
    """The dimension coverage alone misses.

    A claim can have a passing test whose scope is narrower than the claim --
    that is precisely how the W3 round-3 idempotency-key claim reached a commit
    message. Binding the declared scope to the test's own docstring means
    broadening the claim cannot be done without touching the test.
    """
    for path, text in _documents():
        for entry in _falsifiers(text, path):
            if entry.get("test") is None:
                continue
            relative, name = TEST_REFERENCE.fullmatch(entry["test"]).groups()
            docstring = _flow(_docstring(relative, name))
            assert docstring, f"{path}: {name} has no docstring to carry falsifier {entry['id']!r}'s scope"
            assert _flow(entry["scope"]) in docstring, (
                f"{path}: falsifier {entry['id']!r} declares scope {entry['scope']!r}, "
                f"which {name}'s docstring does not restate"
            )


def test_no_two_claims_declare_the_same_scope() -> None:
    """The half of claim-to-scope agreement a machine can actually check.

    The gate binds scope to the named test's docstring, and nothing binds the
    *claim* to the scope -- a semantic relation no test can verify. But its
    commonest failure is mechanical: a scope string copied from another
    falsifier along with its test reference, leaving a claim cited by a test
    that would not fail if the claim were false. That produced a false-positive
    coverage entry in the W4 scope document, inflating its honest 1-of-6 to
    2-of-6. Identical scopes across distinct claims are what that looks like.
    """
    seen: dict[str, str] = {}
    for path, text in _documents():
        for entry in _falsifiers(text, path):
            scope = _flow(entry["scope"])
            if scope in seen and seen[scope] != entry["id"]:
                pytest.fail(
                    f"{path}: falsifier {entry['id']!r} declares the same scope as "
                    f"{seen[scope]!r}; a copied scope usually means a copied test reference "
                    "that does not falsify this claim"
                )
            seen[scope] = entry["id"]


def test_falsifier_coverage_meets_the_pinned_minimum_per_document() -> None:
    """Per document, never pooled.

    A pooled ratio lets a thoroughly falsified document carry an unfalsified
    one, which is exactly backwards: the document with no falsifiers is the one
    that needs the gate.
    """
    for path, text in _documents():
        block = _block(text, path)
        entries = block["falsifiers"]
        target = block["minimum_coverage"]
        gaps = [entry["id"] for entry in entries if entry.get("test") is None]
        coverage = (len(entries) - len(gaps)) / len(entries)
        assert coverage >= target, (
            f"{path}: falsifier coverage {coverage:.2f} is below the {target:.2f} "
            f"this document declares; declared gaps: {gaps}"
        )

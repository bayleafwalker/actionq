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

# Pinned so a drop is a visible edit, the same way the reachability manifest is
# hand-edited rather than regenerated. Raising it is free; lowering it should
# require saying why in review.
MINIMUM_COVERAGE = 0.85


def _documents() -> list[tuple[Path, str]]:
    found: list[tuple[Path, str]] = []
    for root in DOC_ROOTS:
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*.md")):
            text = path.read_text(encoding="utf-8")
            if FALSIFIER_BLOCK.search(text):
                found.append((path, text))
    return found


def _falsifiers(text: str, path: Path) -> list[dict]:
    blocks = FALSIFIER_BLOCK.findall(text)
    assert len(blocks) == 1, f"{path}: expected exactly one falsifiers block, found {len(blocks)}"
    try:
        parsed = json.loads(blocks[0])
    except ValueError as malformed:  # pragma: no cover - failure path is the point
        pytest.fail(f"{path}: falsifiers block is not valid JSON: {malformed}")
    assert isinstance(parsed, list) and parsed, f"{path}: falsifiers block must be a non-empty list"
    return parsed


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


def test_falsifier_coverage_meets_the_pinned_minimum() -> None:
    total = covered = 0
    gaps: list[str] = []
    for path, text in _documents():
        for entry in _falsifiers(text, path):
            total += 1
            if entry.get("test") is None:
                gaps.append(f"{path.name}:{entry['id']}")
            else:
                covered += 1
    coverage = covered / total
    assert coverage >= MINIMUM_COVERAGE, (
        f"falsifier coverage {coverage:.2f} is below the pinned {MINIMUM_COVERAGE:.2f}; "
        f"declared gaps: {gaps}"
    )

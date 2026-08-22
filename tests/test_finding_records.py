"""Per-finding review records are well formed and internally consistent.

The measurement track's item 3 compares a cheap implement tier's first-attempt
pass rate against the expensive baseline.  A rate is only worth comparing if the
records it comes from are honest, so this module enforces the properties that
make them so -- above all that an unmeasured field is null *with a stated
reason* rather than guessed, and that a finding's routing follows from its
classification rather than from whoever wrote the record.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest


ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/evidence"
SCHEMA = EVIDENCE / "finding-record-schema.json"
ID = re.compile(r"^w[0-9]+-r[0-9]+-(g|o)[0-9]{2}$")
CHANNELS = {"pr_gate": "g", "opus_design": "o"}
CLASSIFICATIONS = {"claim": "plan", "line": "implement"}
CAUGHT_BY = {"tests", "review", "probe", "n/a"}
TIERS = {"opus", "cheap"}
REQUIRED = {"id", "round", "channel", "summary", "classification", "routed_to",
            "implemented_by", "first_attempt_pass", "collateral_breakage", "caught_by"}
OPTIONAL = {"unavailable_reason", "notes"}


def _records() -> list[dict]:
    found: list[dict] = []
    for path in sorted(EVIDENCE.glob("*-finding-records.jsonl")):
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            try:
                found.append(json.loads(line))
            except ValueError as malformed:  # pragma: no cover - failure path is the point
                pytest.fail(f"{path}:{number}: not valid JSON: {malformed}")
    return found


def _rounds() -> set[int]:
    seen: set[int] = set()
    for path in sorted(EVIDENCE.glob("*-review-rounds.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                seen.add(int(json.loads(line)["round"]))
    return seen


def test_the_schema_and_at_least_one_record_file_exist() -> None:
    assert SCHEMA.is_file()
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    assert schema["schema_id"] == "actionq/review-finding-record/v1"
    # The baseline definition is the thing item 3 is measured against; a schema
    # that lost it would leave the numbers uninterpretable.
    for key in ("eligible_for_cheap_tier", "first_attempt_pass", "collateral_breakage",
                "escalation_trigger"):
        assert schema["baseline_definition"][key].strip()
    assert _records(), "no finding records to validate"


def test_every_record_is_well_formed() -> None:
    seen: set[str] = set()
    known_rounds = _rounds()
    for record in _records():
        identifier = record.get("id", "<missing>")
        assert ID.fullmatch(str(identifier)), f"{identifier}: malformed id"
        assert identifier not in seen, f"{identifier}: duplicate id"
        seen.add(identifier)

        unknown = set(record) - REQUIRED - OPTIONAL
        assert not unknown, f"{identifier}: unknown fields {sorted(unknown)}"
        missing = REQUIRED - set(record)
        assert not missing, f"{identifier}: missing fields {sorted(missing)}"

        assert record["round"] in known_rounds, f"{identifier}: round is not in any review-rounds record"
        assert record["channel"] in CHANNELS, f"{identifier}: unknown channel"
        assert identifier.split("-")[2][0] == CHANNELS[record["channel"]], (
            f"{identifier}: id channel letter disagrees with the channel field"
        )
        assert f"-r{record['round']}-" in identifier, f"{identifier}: id round disagrees with the round field"
        assert record["classification"] in CLASSIFICATIONS, f"{identifier}: unknown classification"
        assert record["caught_by"] in CAUGHT_BY, f"{identifier}: unknown caught_by"
        assert str(record["summary"]).strip(), f"{identifier}: empty summary"


def test_routing_follows_from_classification_and_not_from_the_author() -> None:
    """A claim-level finding goes to the plan tier by definition.

    Recording routing independently would let a claim-level finding be handed to
    the implement tier by whoever wrote the record, which is the one substitution
    the track exists to prevent.
    """
    for record in _records():
        expected = CLASSIFICATIONS[record["classification"]]
        assert record["routed_to"] == expected, (
            f"{record['id']}: classification {record['classification']!r} must route to {expected!r}"
        )


def test_unmeasured_fields_are_null_with_a_reason_rather_than_guessed() -> None:
    for record in _records():
        identifier = record["id"]
        tier = record["implemented_by"]
        assert tier is None or tier in TIERS, f"{identifier}: unknown implemented_by"
        for field in ("first_attempt_pass", "collateral_breakage"):
            value = record[field]
            assert value is None or isinstance(value, bool), f"{identifier}: {field} must be boolean or null"
        if record["first_attempt_pass"] is None:
            assert str(record.get("unavailable_reason", "")).strip(), (
                f"{identifier}: first_attempt_pass is null without an unavailable_reason"
            )
        else:
            assert "unavailable_reason" not in record, (
                f"{identifier}: carries an unavailable_reason but also a measured first_attempt_pass"
            )
        if tier is None:
            assert record["first_attempt_pass"] is None, (
                f"{identifier}: no fix was applied, so there is no first attempt to pass"
            )


def test_the_baseline_is_reported_as_partial_while_it_is_partial() -> None:
    """The W3 rounds cannot yield a first-attempt rate retroactively.

    Diagnosis and implementation were never separated there, and fixes landed in
    per-round batches. Guessing the missing values would produce a baseline that
    looks comparable and is not, so the schema must keep saying so for as long as
    any record is unmeasured.
    """
    records = _records()
    measured = [record for record in records if record["first_attempt_pass"] is not None]
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    if len(measured) < len(records):
        assert any("partial baseline" in limit for limit in schema["known_limits"]), (
            "records are unmeasured but the schema no longer states the baseline is partial"
        )


def test_every_channel_and_classification_is_represented() -> None:
    """A distribution computed from one channel or one classification would not
    support the comparison item 3 is for."""
    records = _records()
    assert {record["channel"] for record in records} == set(CHANNELS)
    assert {record["classification"] for record in records} == set(CLASSIFICATIONS)

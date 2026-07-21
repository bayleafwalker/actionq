from __future__ import annotations

from pathlib import Path

from actionq.usage_limit import classify_usage_limit, write_handoff


def test_zero_exit_is_never_a_usage_limit_signal():
    signal = classify_usage_limit("claude", exit_code=0, output="rate limit exceeded")
    assert signal.detected is False


def test_ordinary_failure_without_confirmed_phrase_is_not_a_signal():
    signal = classify_usage_limit("claude", exit_code=1, output="Traceback: KeyError('x')")
    assert signal.detected is False


def test_confirmed_claude_rate_limit_phrase_is_detected():
    signal = classify_usage_limit("claude", exit_code=1, output="Error: rate limit exceeded, please try again later")
    assert signal.detected is True
    assert "rate limit" in signal.reason
    assert "rate limit" in signal.evidence.lower()


def test_confirmed_codex_signal_is_detected():
    signal = classify_usage_limit("codex", exit_code=1, output="429 too many requests from upstream")
    assert signal.detected is True


def test_unknown_harness_never_matches_anything():
    signal = classify_usage_limit("some-future-harness", exit_code=1, output="rate limit exceeded")
    assert signal.detected is False


def test_evidence_is_bounded_to_tail():
    huge = "x" * 10_000 + "rate limit exceeded"
    signal = classify_usage_limit("claude", exit_code=1, output=huge)
    assert signal.detected is True
    assert len(signal.evidence) <= 4000
    assert signal.evidence.endswith("rate limit exceeded")


def test_write_handoff_is_bounded_and_durable(tmp_path: Path):
    path = write_handoff(
        tmp_path / "handoff",
        session_id="aqs:abc",
        action_id=42,
        action_type="scope-iterate",
        harness="claude",
        model="claude-sonnet-4-6",
        reason="confirmed usage-limit signal matched: 'rate limit'",
        evidence="rate limit exceeded, resets at 08:00Z",
    )
    assert path.exists()
    text = path.read_text(encoding="utf-8")
    assert "aqs:abc" in text
    assert "resume" in text.lower() and "session.resumed" in text
    assert "rate limit exceeded" in text
    # No stray .tmp file left behind after the atomic rename.
    assert not path.with_suffix(path.suffix + ".tmp").exists()


def test_write_handoff_records_missing_evidence_explicitly(tmp_path: Path):
    path = write_handoff(
        tmp_path / "handoff",
        session_id="aqs:no-evidence",
        action_id=None,
        action_type=None,
        harness=None,
        model=None,
        reason="confirmed usage-limit signal matched",
        evidence=None,
    )
    text = path.read_text(encoding="utf-8")
    assert "(no evidence captured)" in text

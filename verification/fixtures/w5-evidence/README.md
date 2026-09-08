# W5 evidence-capture fixtures

Reserved location for any synthetic fixtures used by
`verification/capture_w5_evidence.py` and its tests
(`tests/test_w5_evidence_capture_tooling.py`).

None exist yet: record 1 needs no fixture (it reads the real, tracked
`docs/contracts/tranche4-reachability-v1.json`), and records 2-7 are
fail-closed skeletons that must never simulate a live capture, so they have
no fixture-shaped test input either -- their tests exercise only the
missing-input and no-backend-wired refusal paths.

Any fixture added here in the future MUST be marked synthetic in its own
content and must never be presented as, or substituted for, a captured
evidence record.

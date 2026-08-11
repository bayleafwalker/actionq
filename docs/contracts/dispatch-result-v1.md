---
doc_id: actionq.dispatch-result
status: implemented
---

# Verified dispatch result and terminal settlement

`dispatch-result/v1` is the one result packet that the verified dispatch path
may use for terminal settlement. It is an immutable JSON object with exactly
these fields:

```json
{
  "contract_id": "dispatch-result/v1",
  "action_id": 42,
  "attempt_id": "aqs:attempt-1",
  "terminal_status": "completed",
  "result_ref": "artifact:sha256:<64 lowercase hex digits>",
  "result_digest": "sha256:<64 lowercase hex digits>",
  "stop_reason": null
}
```

`attempt_id` is not worker-supplied authority: schema v10 mints it with the
current ActionQ claim and returns it in the claim result. Settlement requires
that exact live claim incarnation, so an attempt copied from another action,
an earlier claim, or an invented session is rejected. `result_ref` and
`result_digest` must bind the same immutable artifact bytes. Every terminal
result carries the artifact. `terminal_status` is frozen to `completed`,
`no_change`, `blocked`, `failed`, or `budget_exhausted`. `completed` and
`no_change` map to ActionQ `completed`; `blocked`, `failed`, and
`budget_exhausted` map to ActionQ `failed`. The latter statuses require one of
the registered privacy-safe `stop_reason` values (`cancelled`, `claim-lost`,
`crash-inferred`, `process-exit`, `settlement-failed`, `start-failed`,
`timeout`, `usage-limit`, or `verification-failed`); successful statuses require
`stop_reason: null`. Inline output, transcripts, prompts,
claim proofs, credentials, environment values, and absolute paths are not
part of this contract.

Before the terminal mutation, the ActionQ authority reads `result_ref` from
the configured server-owned durable CAS and re-hashes the bytes against the
locator. A missing, corrupt, or rehashed object fails closed with no row,
claim, or event change. ActionQ then validates the packet shape and binding,
verifies that it identifies the claimed action, and performs the fenced
terminal update in one Postgres transaction. It does not interpret the
artifact's action-specific behavior:

| Terminal status | Action state | Event | Stored result |
| --- | --- | --- | --- |
| `completed`, `no_change` | `completed` | `action_completed` | `result_ref` |
| `blocked`, `failed`, `budget_exhausted` | `failed` | `action_failed` | `result_ref` |

Settlement requires the live claimant identity, matching opaque claim receipt,
matching claim attempt, and unexpired lease. A stale, expired, cancelled,
swept, reclaimed, already-terminal, malformed, or mismatched settlement makes
no lifecycle change or event residue. Cancellation, timeout sweep, and claim
ownership remain ActionQ operations; Sprintctl is not written as part of this
contract.

The `actionctl settle` packet contains only `claim_receipt`, `runner_proof`,
and `result`. The proof is consumed as private input and never enters the
event payload. Legacy `complete`, `fail`, and `reject` commands remain for
their existing compatibility contract; they do not establish a verified
`dispatch-result/v1` settlement.

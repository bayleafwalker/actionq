---
doc_id: actionq.portable-runner
status: implemented
---

# Portable runner contract

## Distribution boundary

`actionq-contracts` owns the dependency-light versioned DTOs, strict field
compatibility, canonical JSON, and digests. The authoritative `actionq`
distribution owns PostgreSQL and lifecycle commands. `actionq-runner` owns
runner signing, child process groups, output redaction, and the private recovery
spool. Neither authority distribution imports the other; the coordinator uses
the installed `actionq-runner` executable over canonical JSON and stdin.

## Runner authority

Claims and cancellation acknowledgements require `runner-auth/v1` Ed25519
proofs. The verified runner id comes from an operator-managed public-key
registry, never a request argument. Proofs bind operation, resource, request id,
and a validity window. Consumed request ids are durably fenced in ActionQ event
history. Claim receipt and the separately minted runner capability are returned
only on the original claim response; only their digests persist. Cancellation
acknowledgement additionally proves the revoked receipt and uses stdin so no
private proof appears in process arguments.

## Execution and recovery

Before starting a child, the coordinator freezes `execution-envelope/v1` and
passes it to `actionq-runner execute`. The runner strips authority and
credential environment variables, owns the child process group, handles
SIGTERM, escalates to SIGKILL within 30 seconds, and waits for process death.
It redacts output in memory before any write, moves it through
`incoming` → `quarantine` → `sealed`, and never treats the spool as canonical
publication. ActionQ settlement is separate; only a successful terminal
reconciliation marks the spool eligible for its bounded retention policy.
Timeout reaping records process state as unknown and leaves an unreconciled
spool intact.

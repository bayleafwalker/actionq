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

## Immutable candidate publication

For an action class with `publish_candidate = true`, the trusted supervisor
publishes only after the scope-iterate kernel has produced a clean, verified
candidate commit. The operator must provision `[global].artifact_root` as
owner-only durable storage outside temporary directories, checkouts, caches,
runtime directories, and the private runner spool.

`actionq-runner publish` creates a verified full-history Git bundle and stores
the exact bundle bytes, released execution/candidate/verification/publication
records, changed-path manifest, verification evidence, and redacted log in an
atomic create-only filesystem CAS. Every object is addressed as
`artifact:sha256:<digest>` over its exact stored bytes. A private canonical
`publication-receipt/v1` object binds the Git source/candidate commit and tree
OIDs to every artifact reference; its artifact reference is the ActionQ
terminal `result_ref`. This receipt is implementation-owned #2032 journal
state, not a new shared runner contract.
Redacted logs are deterministically capped at 1 MiB and the receipt records the
30-day retention policy. This slice does not implement deletion; operators
must preserve active/unreconciled evidence, while integrated or released
bundles and receipts remain pinned indefinitely.

Publication and terminal ActionQ mutation are separate retry boundaries. A
durable journal records bundle, object, completed-receipt, and settlement
stages without claim receipts or credentials. After reclaim, the daemon
recovers a completed receipt for the action and settles it under the new live
claim instead of rerunning the harness. If the completion response is lost,
the daemon reads ActionQ history and acknowledges local settlement only when
the authoritative terminal status and result reference match. The guarantee
is content-idempotent publication plus at-most-once terminal mutation under a
live claim—not exactly-once processing.

The pilot never pushes Git, opens pull requests, promotes pre-#2032 spool
records, deletes canonical bundles/receipts, or performs generalized garbage
collection. A later trusted Git publisher may consume an accepted bundle but
cannot run the harness that created it.

## Second disposable implementation

`actionq-runner oci-execute` is the second implementation of the released
envelope. The #2033 pilot is intentionally offline and deterministic: a
trusted supervisor freezes the exact source as a read-only Git bundle and
starts a digest-pinned image through an explicitly configured rootless OCI
engine. The worker has `network=none`, a read-only root filesystem, private
PID/IPC namespaces, no added capabilities or new privileges, no devices or
engine socket, and no writable host bind. Its only material workspace is a
10 GiB kernel tmpfs; `/tmp`, `/run`, and the trusted control record are
separately bounded.
CPU, memory, disk, PID, and 1,800-second wall-clock ceilings are mandatory.

Before untrusted bytes run, the supervisor reads back engine inspection state
and fails closed if the engine weakened the image digest, UID mapping,
network, mounts, namespaces, capabilities, tmpfs, or resource limits. A trusted
engine seccomp profile is selected explicitly and accepted only when its bytes
match the digest frozen by the coordinator. All Git operations over
worker-controlled state run under the zero-capability worker identity; the PID-1
wrapper kills and reaps every remaining namespace descendant before sealing.
A deterministic container identity is removed after runner death and retried
during daemon restart recovery. The immutable image wrapper exports only a
candidate Git bundle, which the supervisor copies
to private staging and validates in a fresh repository for object integrity,
source ancestry, candidate cleanliness, modes, and the frozen path allowlist.
Publication, registration, settlement, and recovery
then use the same #2032 supervisor path as the devbox implementation. A
successfully reconciled OCI attempt removes the exact container, tmpfs, and
private staging record and records observed destruction; cancelled, crashed,
or unreconciled attempts remain quarantined.

Provider-backed model calls, allowlisted HTTPS egress, Kubernetes Jobs, image
registry rollout, and generalized scheduling are not part of this proof.

## Bounded execution groups

`execution-group/v1` is an immutable projection over ordinary pending ActionQ
actions. A coordinator realizes a content-addressed plan once, preserving the
exact released `execution-envelope/v1` bytes and digest for every ordered
member. Reusing the same `plan_ref` with the identical canonical specification
returns the existing group; any different specification is rejected.

The pilot controls are limited to `max_parallel` (1 through 32),
`continue-independent`, and permanent `stop-new-claims`. Claims serialize on
the group row before capacity is admitted. Stopping leaves pending members
pending but unclaimable; it neither interrupts already claimed members nor
changes their ordinary terminal settlement. Swept or requeued members remain
fenced after a stop.

Groups do not define dependencies, ordering, retries, rollback, group-wide
cancellation, or a group terminal result. Sprintctl remains authoritative for
plan dependencies, readiness, and acceptance. Group reads expose member
identity, status, and envelope digests, never frozen envelope bytes or claim
credentials.

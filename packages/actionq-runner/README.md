# actionq-runner

Secret-free ActionQ child-process supervisor, signed runner identity client,
and private noncanonical recovery spool.

The trusted supervisor can publish a completed candidate using
`actionq-runner publish --packet-stdin`. Publication validates the exact clean
Git candidate and its source ancestry, creates a verified Git bundle, and
writes released contract records, evidence, a changed-path manifest, and the
already-redacted log to an explicit durable create-only content-addressed
store. Artifact references are `artifact:sha256:<digest>` over exact stored
bytes. Private canonical journal events make bundle and record publication
restart-safe; they are recovery state, not canonical artifacts.

The command never pushes Git, opens a pull request, settles ActionQ, or performs
garbage collection. The packet must not contain claim receipts, credentials,
tokens, private keys, database URLs, provider authentication, or unredacted
output. The artifact root must be provisioned as owner-only durable storage;
do not place it in `/tmp`, a checkout, the runner staging spool, or a cache or
runtime directory.

Recovery interfaces are `journal-list`, `journal-recover`, `settlement-ack`,
and `settlement-query`. A settlement acknowledgement is a private,
create-only, idempotent journal event binding the action, immutable journal
digest, terminal status, and result digest. It never changes CAS objects and
contains no claimant credential.

`actionq-runner oci-preflight --engine <executable>` validates the second-runner
host boundary. `oci-execute --packet-stdin` accepts only an immutable image,
the released execution envelope, a registered deterministic command, a clean
remoteless exact-source workspace, sanitized environment, and bounded limits.
It uses an explicitly rootless OCI engine with network disabled, a numeric
nonroot worker with zero effective capabilities, no-new-privileges, private PID/IPC
namespaces, a read-only root, bounded `/tmp`, `/run`, and configurable
`/workspace` tmpfs mounts, and exactly one read-only exact-source bundle bind.
An immutable trusted wrapper runs as root only inside the rootless user
namespace with the exact `CHOWN`, `SETUID`, `SETGID`, `DAC_OVERRIDE`, and `KILL`
capabilities needed for the ownership boundary. It drops the registered command
to the exact nonroot worker UID, seals
a candidate bundle, and holds the container alive while the supervisor copies
and validates it. There is no writable host output mount. The worker receives
no engine socket, device, authority
credential, publication credential, or additional mount. The supervisor
terminates and reaps timed-out or cancelled containers. The supervisor verifies
bundle integrity, strict fsck, ancestry, changed paths, and modes before it
imports the verified commit into the clean, remoteless publication worktree.
The engine seccomp profile is selected explicitly and must match the digest
frozen in the execution packet. Deterministic container naming lets the daemon
remove an attempt container after runner death and reconcile it again on restart.

This pilot deliberately supports deterministic offline commands only. The
ActionQ daemon composes it with the unchanged #2032 publication and settlement
path; provider access, egress allowlisting, or deployment are not implemented.

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

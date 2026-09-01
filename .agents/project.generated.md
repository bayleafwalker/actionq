<!-- agentops-render: DO NOT HAND-EDIT
     project_id: 981b2073-d7af-4c28-bff3-3cf807495fba
     project: vuoro
     member: actionq
     render: full
     source_bundle_sha256: 312fc5ea6441d27dbbf3071bb96c6a31d4c0ff1433d3ba9512fa8c29d8c0f3f4
     tool: agentops-render/v1
-->

# Vuoro project scope

This repository participates in the `vuoro` multi-repository project. The
project is a read and instruction projection; each member repository remains
the authority for its own runtime behavior and Git history.

- Canonical binding and shared sources live in the `agentops` home repository.
- Cross-cutting project work is tracked in the agentops sprintctl backlog.
- Use `sprintctl usage --context --project --json` and
  `sprintctl next-work --project --json --explain` from a materialized project
  folder. Every union row must retain its `origin_repo`.
- Direct repository sessions remain supported. Omitting `--project` must keep
  the repository-local sprintctl behavior unchanged.
- Project instructions are baseline guidance followed by member-owned
  overrides. The member's authored `AGENTS.md` remains authoritative for local
  workflow and safety constraints.

Before a cross-repository work window, synchronize the derived project folder.
Treat dirty, divergent, or unexpectedly branched member worktrees as a stop
condition; resolve them through the owning repository rather than resetting
them from project tooling.

## Ecosystem ownership and safety boundaries

- `agentops` owns reusable dispatch templates, project bindings and render
  tooling, cross-repository guidance, and the cockpit application source.
- `sprintctl`, `kctl`, and `actionq` own their respective runtime semantics and
  state. Do not add raw cross-tool database writes or cross-tool transactions.
- The former `outctl` member is retired from active Vuoro scope. Its repository
  remains a frozen discovery artifact; new harness-evidence work belongs at the
  native runtime boundary and targets standard OpenTelemetry plus Langfuse or
  Phoenix with object storage rather than a new evidence product.
- Inspect declared `risk_surfaces` before changing queue, claim, lease, retry,
  recovery, projection, publication, reconciliation, or backend-parity paths.
  `full` is a sequence of scoped actions, not blanket mutation authority.
- Keep browser-facing cockpit writes behind documented owning APIs. Project
  scope does not authorize cluster reconciliation, image publication, or
  deployment changes.
- Reusable dispatch behavior stays canonical in agentops. Express a member's
  true difference in its own `.agents/overlays/` fragment instead of copying a
  shared skill body.

Check generated guidance with the deterministic renderer in agentops. Missing,
stale, or hand-edited output is regenerated from canonical sources; it is never
merged manually.

## Portfolio disposition

Generated from `vuoro:docs/direction/disposition-register.yaml` by
`agentops:templates/dispatch/scripts/render_disposition_fragment.py`. Do not edit
here; change the register and re-render.

The register records status separately from intention: intention is what a plan
wants, status is what the artifact supports. Where a document and the register
disagree about whether something exists, the register was checked at the artifact.
Each entry's waking condition, evidence and supersession live in the register.

### retired

- `actionq-dispatcher` — A deliberate retirement tombstone - a 34-line Click CLI keeping the dispatcher-once script…
- `hostproto-rust-spec-repo` — An engine-neutral browser-host contract as a frozen prose/YAML spec package (~1600 lines)…
- `meta-layer` — A multi-agent coordination layer in which one session dispatches and reconciles explorers,…
- `outctl` — Bounded command-output capture tool, formerly a supplemental Vuoro component, now a frozen…
- `sprintctl-orchestrator` — A three-file design workspace (ADR-001, README, AGENTS.md; 469 lines) arguing for a separate…
- `vuoro-bounded-output-starter` — A non-versioned handoff scaffold for outctl with five JSON schemas, a Phase 1 implementation…

### frozen

- `cluster-alignment-mvp` — A read-only executable proof for bounded native-runtime cluster investigation and stateless…
- `flowlab` — A deliberately disposable single-day lab that read existing auditctl NDJSON to measure where…
- `takeover-experiment` — A 702-file executed experiment testing whether a coordinated multi-session takeover beats a…

### deferred

- `effect-grant` — Authorisation for consequential effects, existing at three levels at once - implemented as a…

### spec-only

- `agent-profile-revision` — Prose-only versioned specification for a compiled, flat, content-addressed effective agent…
- `experiment-record` — Prose-only ledger object for change and comparison memory (hypothesis, baseline, challenger,…
- `recipe-revision` — Prose-only versioned specification for a pinned, task-class-specific execution recipe binding…
- `work-release` — Named in direction section 5 and listed in section 15 as needing a minimal schema - the…

### hold

- `acceptance-lab` — Deterministic offline settlement evaluator - scores a CandidateOutput against a versioned…
- `beads-comparison` — Not a repo or a tool - a completed-then-paused clean-room evaluation of external…
- `bindery-core` — Two unrelated subsystems merged into one Go module on 2026-08-26 - a Kubernetes operator (9…
- `bindery-ra2-adapter` — A GPL-3.0 .NET 8 Windows adapter, the client-side half of the external-runtime experiment for…
- `cred-broker-public` — Public-facing extraction of cred-broker (own Dockerfile, SECURITY.md, CONTRIBUTING.md,…
- `datacluster` — Cost-optimized Apache Spark platform on Hetzner Cloud (Talos, Kubernetes, Terraform, Cilium,…
- `hostproto-a2a-worker` — An HTTP/JSON-RPC A2A 1.0 agent (Express, @a2a-js/sdk 1.1.0, agent card, task store, healthz)…
- `hostproto-consumer-vuoro-evidence` — The consumer the bounded proof turns on - a core reducer/model/decision path over generic…
- `hostproto-semantics-and-adapters` — A JSON-Schema vocabulary for host interaction (intent, receipt, observation, evidence-ref,…
- `knowledge-base` — A committed prose knowledge store (concepts/, domains/, generated/, knowledge-map.md,…
- `render-fabric` — A local GO-FAKE proof for bounded batch execution - typed requests, SQLite broker, finite…
- `vuoro-cloud` — Multi-tenant hosting product - FastAPI control plane, public gateway minting Ed25519 identity…

### Open claims

Each was re-derived from the artifact and did not reproduce as stated. Do not cite the original figure or sequence work against it.

- **Served next-work works, but the product's first promised capability - session resume -…** True only in its weakest reading (no served operation spelled 'session resume' exists) and false under the decision record's own test, the…
- **Cost telemetry is overstated by 5.4-5.9x.** Overstatement is real; the figure is not.
- **69% of recorded session events are fixtures, and agentops consumed 31% of measured effort.** The contamination is real and far worse than 69%; the 31% is unsupported and contradicted by every recomputation.
- **Native runtimes increasingly own session resume, subagents, hooks, skills and…** Two-thirds evidenced, one-third assumed.
- **The multi-agent coordination layer should be frozen (stop developing it).** The conclusion is right and v1's reasoning was wrong twice.

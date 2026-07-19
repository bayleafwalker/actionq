<!-- agentops-render: DO NOT HAND-EDIT
     project_id: 981b2073-d7af-4c28-bff3-3cf807495fba
     project: vuoro
     member: actionq
     render: full
     source_bundle_sha256: 304fbdd4f9b61dab6da9fc545eca2964615d07797040ec08ecdf96dac8690b05
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

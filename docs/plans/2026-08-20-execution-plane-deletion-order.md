# Execution-plane deletion order (step 3)

**Recorded:** 2026-08-20 · Supersedes nothing; operationalises step 3 of `HANDOFF.md`.
Derived from a static import closure over `actionq/` **plus** the subprocess and out-of-process
boundaries that closure alone does not see.

Step 3's constraint is *"delete in an order that keeps devbox working."* This document is that
order, and — more usefully — the list of things that **look** deletable and are not.

---

## The safety property that makes any of this possible

`actionq-agent-tools-refresh.service` reinstalls devbox's `actionq` uv tool from
`/projects/dev/actionq` **on devbox's own checkout**, daily, `Before=actionq-dispatch.service`.
It is **fail-closed** on two conditions (`gitops-nixos/scripts/update-agentops-tools.sh`):

```bash
ACTIONQ_REQUIRED_REVISION=ab0dfb7655740c83a0b15255277c2acada9819ef   # declared in
                                # gitops-nixos/modules/system/actionq-dispatch.nix:338
# refuses if HEAD != required revision, and refuses if the checkout is dirty
```

Three facts follow, and they are the reason deletion on `main` is safe at all:

1. **Devbox has its own checkout**, not a shared mount of this working copy. Branch switching
   here does not touch it.
2. **Devbox is pinned to `ab0dfb7`, which is not an ancestor of `main`** and never was — it lives
   on `agent/p2.2-schema-runtime-actionq`. The deployed daemon runs code from a feature branch.
3. Adopting anything from `main` therefore requires **two explicit, reviewed acts**: moving
   devbox's checkout, *and* bumping the pin in `gitops-nixos`. Nothing propagates by itself.

**Deleting on `main` cannot break the running devbox daemon.** What it can break is the *ability
to bump the pin later* — which is why the order below is still ordered.

---

## Reachability map

Roots are the console scripts plus `vuoro.py`, which vuoro-service imports out of process.

| Module | LOC | Reached from |
|---|---:|---|
| `daemon`, `daemon_audit`, `daemon_claim`, `daemon_clients`, `daemon_config`, `daemon_lifecycle`, `daemon_routing`, `daemon_runner`, `daemon_runtime` | 2,901 | **daemon only** |
| `harnesses/*` (7 modules), `harness_profiles` | 621 | **daemon only** |
| `routing`, `scope_iterate`, `usage_limit`, `git_evidence` | 958 | **daemon only** |
| `completion_outbox` | 659 | daemon + own console script |
| `cli` | 446 | actionctl only |
| `server` | 416 | server only |
| `vuoro` | 948 | vuoro-service (out of process) |
| `application_*`, `action_resource`, `completion_log`, `managed_dispatch`, `runner_auth` | 2,082 | actionctl, server, **and vuoro** |
| `db`, `schema`, `cas` | 3,769 | everything |

**Daemon-only total: 4,480 LOC.** That is the execution plane proper, and it is gated on exactly
one decision (tranche 3).

---

## Four things that look deletable and are not

Each of these would have been deleted by following the import graph alone.

1. **`cli.py` is a daemon dependency.** Not by import — by subprocess. `daemon_clients.py` shells
   out to `actionctl_bin`, and the tmux unit runs `actionctl check-compatibility` before the
   daemon starts. Deleting `cli.py` breaks devbox despite zero import edges.
2. **`actionq/migrations/` is live.** Its `__init__.py` is one line and nothing imports the
   package, so closure reports it unreachable. It holds the twelve versioned `.sql` assets
   `schema.py` applies as package data.
3. **`vuoro.py` is unreachable from every console script** and is the single most important module
   to keep — it *is* the federation surface the whole goal is aimed at.
4. **`server.py` backs a declared cluster deployment.**
   `appservice/clusters/main/kubernetes/apps/actionq-server/` with `prune: true` and a health
   check on `Deployment/actionq-server` in namespace `vscode`, running
   `ghcr.io/bayleafwalker/actionq-server@sha256:2d5121cf…` — pinned by digest, so the *running*
   pod does not track `main`. **Not verified live:** this host has no appservice kubecontext
   (only `kind-*`), so what is established is that it is *declared* deployed, not that it *is*.

---

## Order

### Tranche 1 — done (2026-08-20)

**`session_wrapper.py`, 970 LOC + console script + 20 tests.** Zero in-package importers; no
reference in `gitops-nixos`, `appservice`, or any devbox unit; its state directory on devbox
(`~/.local/state/actionq/session-wrapper/`) has been empty since 2026-07-21. `git_evidence.py`
was extracted from it in #1114 precisely so daemon recovery would not depend on it.

`completion_outbox.DEFAULT_OUTBOX_PATH` still contains the string `session-wrapper`. That is
**load-bearing state**, not a stale reference: the deployed daemon reads that exact path.

### Tranche 2 — `server.py` (416) + `runner_auth` if it falls out

**Gate:** remove the `actionq-server` Kustomization from the cluster, or confirm it is not
reconciling. `prune: true` means deleting the directory deletes the workload — an outward-facing
change requiring an explicit decision, not a side effect of a source cleanup.

### Tranche 3 — the daemon-only execution plane (4,480 LOC)

**Gate:** devbox stops running `actionq-dispatch.service`.

This is the tranche the whole goal is about, and F9 is the argument for it: seventeen actions
across the queue's entire history, all hand-created, no dispatch planner on any host. There is no
standing demand for a claim loop. Note the fence proved the daemon *can* be stopped without
anything noticing (F4–F8), and it is currently running only because it was resumed by decision.

Deleting it also deletes the four harness adapters, whose qualification records (N1–N13) survive
in `docs/evidence/` — which was A19's point: **the record is the durable asset, not the adapter.**

### Tranche 4 — the lease/claim surface inside `db.py` / `schema.py`

Last, as `HANDOFF.md` has always said. `db.py` (2,051) and `schema.py` (1,588) are reached by
every root including `vuoro`, so this is extraction, not deletion, and it is entangled with
schema versions the deployed daemon reads. Do not start it before tranche 3.

---

## What remains at the end

`vuoro.py` plus the `application_*` / `action_resource` / `completion_log` layer and the
non-queue parts of `db.py`/`schema.py`: work identity, relations and revisions, authority,
evidence requirements and acceptance, references to external executions, reconciliation, backend
qualification, and a narrator-facing read/write surface.

Both remaining gates are **decisions, not measurements**. The evidence for each is already
recorded; nothing further can be learned by waiting.

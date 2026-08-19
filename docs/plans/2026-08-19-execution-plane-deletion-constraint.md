---
doc_id: actionq-execution-plane-deletion-constraint
status: proposed
date: 2026-08-19
---

# Do not own an agent execution plane

**Constraint adopted:** execution is mature enough *in the products around us* that we should
not own it. Cross-product coordination is immature, so that layer stays thin enough to throw
away rather than trying to complete the market ourselves.

This supersedes the earlier "the execution plane is market mature" framing, which was
directionally right but too broad. Process/job execution is mature. Coding-agent *session*
execution is mature **inside individual products**. Cross-provider, subscription-preserving
orchestration is not yet a clean commodity — and that gap is not an invitation to build one.

## What this costs, measured rather than asserted

Counted 2026-08-19 in this repo:

| Surface | LOC | Verdict |
|---|---:|---|
| `daemon_runner`, `session_wrapper`, `daemon_{clients,config,lifecycle,routing}`, `server`, `application_dispatch`, `routing`, `usage_limit`, `scope_iterate` | **~5,030** | execution plane — candidate for deletion |
| lease / claim / heartbeat / worker-registration (`application_claim`, `daemon_claim`, plus large parts of `db.py` 2,051 and `schema.py` 1,588) | large, entangled | same category, harder to extract |
| `execution_contract`, `execution_boundary`, `context_policy`, `acp/backends` | **~1,005** | **own** — the durable semantics |
| `acp/v1.py` | 760 | integrate — a thin adapter, already replaceable by construction |

**The homemade execution plane is roughly five times the size of the part worth owning.** That
ratio is the argument; nothing about it depends on taste.

## What survives, and why

`ExecutionEnvelope`, the invariants, `execution_boundary` and `context_policy` are **not**
execution-plane code. They encode acceptance semantics and make invalid states
unrepresentable — including the A4 fix, where an execution without an explicit model silently
bound a hosted default. That is precisely "own what is peculiar to the workflow and must
remain true across tool changes", and it already lives in `packages/actionq-runner`, separate
from the daemon.

Backend qualification survives for the same reason. The ACP work (A12–A19) produced no
execution capability and was never supposed to; it produced a characterization of what
Claude Code ACP can and cannot attest, which is workflow-specific, outlives the adapter, and
regenerates in minutes via `verification/*.py`. That is the right kind of custom work.

## Three hazards this plan must not walk into

**1. There is a deployed daemon.** `gitops-nixos` `hosts/devbox` enables `actionqDispatch`
with a 60 s heartbeat, a `budget_daily_usd` gate, a `PAUSED` runtime fence and direct routes
to the cluster ActionQ Postgres LB. Deleting the worker daemon is a live-service migration on
a host that runs `agentUnattended = true`, not a code deletion. Sequence it explicitly, and
keep the PAUSED fence working until the last consumer is gone.

**2. Subscription preservation narrows the provider set more than a capability table
suggests.** OpenCode's own documentation states Anthropic does not permit Claude Pro/Max
through it, while ChatGPT subscriptions are supported. GitHub Agentic Workflows run
third-party engines on *provider credentials*, not on personal subscription entitlements. So
for subscription-backed work the providers are **not interchangeable**: only Claude's native
surfaces spend the Claude subscription, only Codex's spend the Codex one. The federation layer
must model that as a hard property of a lane, not smooth it away as a capability difference.

**3. Research-preview surfaces raise the value of qualification, not lower it.** Claude Code
Web and Remote Control are labelled research preview. Coupling weakly is right; so is
re-running the qualification probes when they change. A characterization is only an asset
while it is current.

## One place the deletion criterion points the other way

"Force every table to justify itself" is the right instinct, and it argues for externalizing
scheduling, session state, retries and process supervision. It does **not** obviously argue
for moving *work state* to GitHub Issues/Projects.

The criterion is: *if an external product could replace a component without changing the
meaning of the workflow, that component should not contain much custom code.* Adopting GitHub
Issues as the work store means adopting GitHub's meaning of open/closed/assigned in place of
`ready | claimed | blocked | accepted | rejected | superseded | integrated`. That is the one
category explicitly listed as unsafe to delegate, because it is the part that embodies the
process. Externalize the *rendering* and the *linking* to GitHub; keep the semantics.

## What is actually left

```text
work identity + relations + revisions
authority (who may establish, mutate, approve, or merely observe)
evidence requirements and acceptance
references to external executions (provider, handle, status projection)
reconciliation of those back into work state
backend qualification
a narrator-facing read/write surface
```

No worker daemon. No queue. No leases. No fan-out engine. Fan-out is declared
("these five items are independent") and executed by whichever backend is native to each.

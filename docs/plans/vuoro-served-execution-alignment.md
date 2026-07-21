---
doc_id: actionq-vuoro-served-execution-alignment
status: ratified
ratified_at: 2026-07-21
ratified_by: operator
governing_decision: agentops/docs/plans/agentops/vuoro-served-substrate-plan.md
---

# Actionq alignment with the Vuoro execution module

Actionq remains the sole authority for queue state, claims, leases, dispatcher
policy, and session lifecycle. The Vuoro execution adapter exposes that
authority through the common handshake, catalog, and invocation envelope; it
does not introduce a second queue or let the service shell write actionq tables
directly.

## Required changes

- Extract adapter-safe application handlers behind the existing `actionctl`
  contract and register domain-qualified execution operations.
- Keep action types opaque to actionq storage while moving cross-machine
  dispatch/batching rules out of workstation installations.
- Convert the current manual production `actionctl migrate` step into an
  appservice-controlled migration job. Runtime service roles cannot run DDL.
- Publish execution API/schema compatibility through the Vuoro handshake.
- Preserve machine-local runner effects; runners receive environment records,
  perform work locally, and report evidence/session receipts.

The persistent `vuoro-dev` environment must exercise claim, renew, timeout,
dispatch, duplicate retry, and service restart histories against an isolated
queue. No development identity may reach the production queue.

If Windmill is introduced later, actionq first claims the governed action and
invokes Windmill as a backend. Windmill returns an execution reference and
material receipts; it never independently schedules the same work.

## Non-goals

- a universal cross-domain queue;
- offline claim or lease acquisition;
- embedding machine runner effects in the service pod;
- a second scheduler writable alongside actionq;
- appservice-specific logic in the actionq core.

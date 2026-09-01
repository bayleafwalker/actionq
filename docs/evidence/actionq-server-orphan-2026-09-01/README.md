# actionq-server: the orphan, and what it was

**Deleted from the cluster 2026-09-01.** The Deployment, Service and ConfigMap were
removed from namespace `vscode`; a cluster-wide sweep afterwards found no object of
any kind named `actionq-server`. Cluster state and appservice git now agree.

Captured 2026-09-01, immediately before deletion. These three manifests were read
back from the running cluster, not from a source repository, because no source
repository declared them any more.

## What happened

`appservice/clusters/main/kubernetes/apps/actionq-server/` was removed, and with it
the Flux Kustomization that owned these objects. `prune: true` collects what a
*live* Kustomization owns; deleting the Kustomization itself removes the owner and
abandons the objects rather than collecting them. So the Deployment kept running.

It carried `kustomize.toolkit.fluxcd.io/name: actionq-server` and
`…/namespace: flux-system` to the end — labels naming a Kustomization that no longer
existed. Age at deletion: 116 days for the Service, 120 for the ConfigMap, with the
pod pinned by digest at
`ghcr.io/bayleafwalker/actionq-server@sha256:2d5121cfc0c35c54fb872bab1bdfaa00709573af09f8205abeb58330c93b4e2d`,
so the running image tracked no branch and no tag.

This inverts the concern recorded in `docs/plans/2026-08-19-execution-plane-deletion-constraint.md`.
The worry there was that deleting the source directory would delete the workload as
a side effect. The opposite happened, and it is worse in one specific way: a pod
served for 116 days with no source of truth, and nothing would have reported it.

## Why deleting it was safe

- **Nothing referenced it.** A sweep of every Deployment, StatefulSet, CronJob and
  ConfigMap in the cluster found exactly two objects containing the string
  `actionq-server`: the Deployment itself and its own ConfigMap.
- **The cockpit does not call it.** `agent-cockpit` in the same namespace takes
  `ACTIONQ_URL` from the `actionq-runtime` secret, whose keys are `username`,
  `password` and `uri` — a PostgreSQL credential. It talks to the database directly.
- **It served nothing.** 11,520 log lines in the last 24 hours, every one of them
  `GET /health` from the kubelet probe. Non-probe requests in 24 hours: zero.

## Restoring it, if that is ever wanted

`kubectl apply -f` these three files into namespace `vscode`, then remove the
`kustomize.toolkit.fluxcd.io/*` labels or recreate a real Kustomization to own them.
Do not restore them as-is under those labels: an object labelled for a Kustomization
that does not exist is exactly the state this record documents.

The image digest above is the one that was running. It is pinned, so the artifact is
still resolvable from the registry independently of this repository.

## Related

- `docs/plans/2026-08-20-execution-plane-deletion-order.md` — tranche 2, whose gate
  this settles, and the "Gate status, measured 2026-09-01" section appended to it.
- The portfolio disposition register at `vuoro:docs/direction/disposition-register.yaml`.

## This completed an intended retirement, it did not start one

The retirement was planned and sequenced in git on 2026-08-20, and the sequencing
was careful. `apps/actionq-db/app/kustomization.yaml` still carries the comment:

> `actionq-server` app on 2026-08-20 so they survive that app's retirement.
> ... This move must reconcile BEFORE actionq-server is removed.

The database resources were deliberately moved out of the app so they would outlive
it, and `apps/agent-cockpit/app/deployment.yaml:172` records the same retirement
date. So every step of the plan happened except the last one, and the last one
failed silently: removing the Kustomization removed the owner, and the objects were
abandoned rather than collected.

That is the finding worth keeping. This was not a forgotten workload; it was a
correctly-sequenced retirement whose final step could not report that it had not
happened. Nothing in the cluster and nothing in git disagreed — the workload simply
kept running for 116 days with no source of truth.

## What still references the name, and why that is fine

`grep actionq-server` over appservice still returns hits. None is a dependency on
the deleted workload:

- **Schema-migration Jobs** (`apps/actionq-db/app/actionq-schema-migrate-v*.yaml`,
  `apps/vuoro-shared-db/app/vuoro-execution-migrate-v*.yaml`) run the
  `ghcr.io/bayleafwalker/actionq-server` **image**. An image reference resolves from
  the registry and never depended on the Deployment or the Service. These keep
  working.
- **Comments** in `agent-cockpit` and `actionq-db` recording the retirement.
- **Historical docs** under `docs/migrations/`, `docs/runbooks/` and
  `docs/training/health-checks/`, describing the cluster as it stood on their dates.

None of these should be edited. Deleting the workload does not make a migration
Job's image reference stale, and rewriting a dated health-check record to stop
naming what it found would falsify it.

## Method note

The deletion was performed with `kubectl` rather than through git, which is the
wrong instrument for a Flux-managed cluster: the cluster is a projection of the
repository, and changes belong in the repository. It is defensible only in this
narrow case — the objects had no Kustomization, so there was no git change that
could have pruned them, and `prune: true` collects only what a live Kustomization
owns. Anything reachable by a git change should be changed in git.

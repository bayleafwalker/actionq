# actionq-server: the orphan, and what it was

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

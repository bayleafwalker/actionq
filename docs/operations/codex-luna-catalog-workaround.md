# Codex Luna V2 catalog workaround

Codex CLI releases affected by
[openai/codex#35097](https://github.com/openai/codex/issues/35097) and
[openai/codex#36294](https://github.com/openai/codex/issues/36294) advertise
Luna as MultiAgent V1 even when the effective runtime can execute Luna on V2.
The V2 `spawn_agent` compatibility filter consequently removes Luna from a
Sol or Terra parent session.

ActionQ provides a temporary, default-off workaround at the Codex harness
launch boundary:

```toml
[harnesses.codex]
bin = "codex"
catalog_workaround = "luna-v1-to-v2"
```

For every configured Codex dispatch, the child wrapper runs `codex debug
models` under the same Unix identity that will run Codex. It accepts exactly
one `gpt-5.6-luna` record whose `multi_agent_version` is `v1`, changes only
that field to `v2`, writes a mode `0600` temporary catalog, and supplies it as
one argv-safe `model_catalog_json` override. The file is removed after normal
exit, caught termination signals, or a Codex start failure. An uncatchable
host/process `SIGKILL` can still leave an OS-temporary file for ordinary
temporary-directory cleanup.

The wrapper fails closed before the model turn when catalog refresh fails,
JSON is malformed, Luna is missing or duplicated, or its version is neither
`v1` nor `v2`. It never changes the selected model and never falls back to
Terra (or any other model); fallback remains an explicit routing decision.

When the refreshed native catalog already marks Luna `v2`, the wrapper logs
`native-v2-bypass` and launches Codex without `model_catalog_json`. This is the
automatic retirement path. After a native Sol/Terra-to-Luna spawn has been
verified without the override, remove `catalog_workaround` from configuration.

Lifecycle and crash-recovery routing provenance records only the stable name
`luna-v1-to-v2`. The wrapper adds one sanitized status line (`applied` or
`native-v2-bypass`) to harness output. Neither surface records the catalog,
its contents, or its temporary path.

This workaround replaces the catalog only for a single child lifetime. Do not
deploy a static patched catalog under `/etc` or a home directory: it can mask
later upstream catalog changes.

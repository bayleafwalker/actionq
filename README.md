# actionq

Operational-domain action queue for deterministic agent dispatch.

## Setup

```bash
export ACTIONQ_URL='postgresql://...'
export ACTIONQ_SCHEMA=actionq
actionctl migrate
```

`actionctl` is the public contract. Consumers should not import the package or
write directly to the database.

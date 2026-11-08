# VM Redis transport

## Host requirements

Use Node.js 20 or newer and `ioredis` 6.0.0 on the existing always-on host. Verify
the actual database endpoint and its TLS port. If the host requires an HTTP
egress proxy, verify that its permitted CONNECT path reaches that endpoint.
`HTTPS_PROXY` or `HTTP_PROXY` is reused. TLS certificate validation stays enabled.

Keep exactly one supervised consumer for `qq:queue` and `qq:processing`. The
existing service supplies supervision. Retire competing legacy HTTP consumers
before starting the direct consumer. Existing outgoing work remains on its
original SDK and outbox path.

## Private configuration

The existing `QQ_POLL_SECRET` decrypts the original AES-GCM queue envelopes.
Deliver native Redis credentials to a private JSON file with these fields.

```json
{"host":"your-database.upstash.io","port":6379,"password":"your-private-password"}
```

The example host patch reads `~/.config/qq-bridge/redis.json`, or the file selected
by `QQ_REDIS_CONFIG_FILE` in the service environment. Keep credentials, temporary
handoff keys and sealed configuration attachments outside source control and
memory snapshots. A new host needs a fresh authorized credential handoff.

## Incremental host patch

Read the actual bridge source first. The reference installer recognizes the
existing `getDispatcher`, `writeInbox`, `pollOutbox`, `connectBot`, HTTP polling
and token proxy boundaries. It requires the reviewed source SHA256 and rejects
unrecognized source changes before replacing the bridge. For a different host,
adapt those boundaries while preserving its supported message shapes.

```sh
cd /path/to/existing/qq-bridge
npm install --save-exact ioredis@6.0.0 --ignore-scripts
# NODE_PATH is unnecessary. Copy the vm folder beneath this existing project
# so the native Redis dependency resolves from its node_modules.
node vm/check-redis.mjs "$HOME/.config/qq-bridge/redis.json"
# The check performs PING and LLEN only.
node vm/install-transport.mjs ./bridge.mjs REVIEWED_SOURCE_SHA256
```

Copy `vm/redis-queue.mjs` and `vm/durable-inbox.mjs` alongside the actual bridge
through the installer. Perform the service's normal restart, then read back one
active process, authenticated TLS, a fresh healthy status and the absence of
further `/api/qq-wait` requests. The host's normal incoming events exercise the
existing worker and outgoing paths. Use synthetic fixtures only in isolation.

## Delivery and recovery

`BLMOVE qq:queue qq:processing RIGHT LEFT 25` atomically claims the oldest event.
The single consumer checks the oldest processing item first after reconnect.
Offline command queues and automatic unfulfilled-command resends are disabled.
Connections have bounded setup and command timeouts with a five-second retry
delay. Idle waits update service health on each return.

Keep message normalization and incoming empty-message handling in the existing
host. Acknowledge only after durable local handoff. The reference inbox writer
uses a message-ID-derived filename, atomic rename and fsync. Look in both inbox
and processed before treating recovery as new work. Update the host's seen IDs
after the durable file is present. Failures leave the processing entry intact.
Malformed encrypted envelopes move atomically into `qq:invalid` without exposing
their decrypted content in logs.

## Vercel and Blob cleanup

Publish HTTP polling retirement only after the VM is connected. The webhook and
short token proxy remain active. Both the former waiting endpoint and legacy
queue-drain/delete actions return 410 without accessing Redis. Requests still
require the existing poll authentication. The VM sends the token secret in its
`x-poll-secret` header.

Check Blob operations by store and recent date range, then audit actual scripts,
processes and scheduled tasks for remaining list/put calls. If the old QQ Blob
store is unused, disconnect its project binding and remove unused store settings
from the active deployment. Preserve stored data until its disposition is known.
Deleting data or deployments cannot erase historical usage. Record any recovery
date shown by the platform separately from the live QQ transport state.

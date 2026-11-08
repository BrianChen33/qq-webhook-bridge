# Direct Redis bridge implementation plan

The owner approved moving the existing QQ bridge's wait off Vercel and resolving obsolete Blob usage.

## Design

Vercel keeps signature verification, encrypted enqueue and the existing short QQ token proxy. The VM consumes the same encrypted list through verified native Redis TLS, using its existing permitted egress proxy. An item first moves atomically into a processing list. Acknowledgement follows successful durable local handoff. Restart recovery uses the processing list and existing message deduplication. Malformed ciphertext moves to a separate encrypted failure list.

## Implementation

- [ ] Read the actual host bridge and preserve SDK dispatch, inbox/outbox behavior, stickers and token handling.
- [ ] Add a reusable VM queue client and a hash-bound host patch. Require one supervised consumer. Include bounded connection/command deadlines and reconnect backoff.
- [ ] Retire Vercel's waiting and destructive legacy polling routes after the VM is ready. Retain the authenticated token action.
- [ ] Disconnect the obsolete Blob project binding and remove obsolete Blob configuration from the active deployment. Historical usage remains in Vercel's accounting window.
- [ ] Run one final isolated acceptance pass for FIFO ordering, ciphertext compatibility, empty messages, failed handoff/restart, acknowledgement and endpoint retirement. Keep synthetic events outside production queues.
- [ ] Publish both the bridge implementation and companion-kit deployment/inheritance guidance, verify remote commits, deploy the host patch and read back the actual service and subsequent request activity.

## Constraints

Secrets travel only in a recipient-key-encrypted attachment to Muse and private host configuration. Public repositories contain generic code and instructions. Existing host customizations and private data stay outside the public source. Reuse the existing service, hook and scheduling cadence. Apply the owner's single final isolated-test preference and avoid new rollback infrastructure.

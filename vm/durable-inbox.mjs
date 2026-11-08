import { createHash } from 'node:crypto';
import { closeSync, existsSync, fsyncSync, openSync, renameSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

export function persistInbox(directory, messageId, payload) {
  const digest = createHash('sha256').update(String(messageId)).digest('hex');
  const name = `redis_${digest}.json`;
  const target = join(directory, name);
  if (existsSync(target) || existsSync(join(directory, 'processed', name))) return false;
  const temporary = `${target}.tmp`;
  const descriptor = openSync(temporary, 'w', 0o600);
  try {
    writeFileSync(descriptor, JSON.stringify(payload, null, 2));
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
  renameSync(temporary, target);
  // The bridge runs on Linux. Directory fsync makes the rename durable before
  // the corresponding Redis acknowledgement, including after a host restart.
  if (process.platform !== 'win32') {
    const parent = openSync(directory, 'r');
    try { fsyncSync(parent); } finally { closeSync(parent); }
  }
  return true;
}

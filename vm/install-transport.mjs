import { createHash } from 'node:crypto';
import { copyFileSync, readFileSync, renameSync, statSync, writeFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { basename, dirname, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

function replaceOnce(source, before, after) {
  if (!source.includes(before) || source.indexOf(before) !== source.lastIndexOf(before)) {
    throw new Error('bridge_patch_source_changed');
  }
  return source.replace(before, after);
}

function replaceSection(source, start, end, replacement) {
  const first = source.indexOf(start);
  const last = source.indexOf(end, first + start.length);
  if (first < 0 || last < 0) throw new Error('bridge_patch_section_missing');
  return source.slice(0, first) + replacement + source.slice(last);
}

export function patchBridge(source) {
  let updated = replaceOnce(source, 'import { promisify } from "util";', `import { promisify } from "util";
import { homedir } from 'node:os';
import { consumeRedisQueue, loadRedisConfig } from './redis-queue.mjs';
import { persistInbox } from './durable-inbox.mjs';`);
  updated = replaceOnce(updated, '  markSeen(msg.messageId);\n', '');
  updated = replaceOnce(updated,
    '  const name = `${Date.now()}_${msg.messageId.slice(-8)}.json`;\n  writeFileSync(join(INBOX, name), JSON.stringify(file, null, 2));',
    `  const created = persistInbox(INBOX, msg.messageId, file);
  markSeen(msg.messageId);
  if (!created) return;`);
  updated = replaceSection(updated, 'async function pollWebhook() {', 'async function connectBot(', '');
  updated = replaceSection(updated, '  log("Webhook poller READY (long-poll mode)");', '  // Keep the process alive;', `  log("Webhook receiver READY (direct Redis/TLS transport)");
  setStatus("connected", { app_id: appId, transport: "redis-tls" });
  setInterval(pollOutbox, 2000);
  redisQueueLoop().catch((error) => {
    log("Redis consumer stopped:", error.code || error.name);
    process.exit(1);
  });
`);
  updated = replaceSection(updated, '// Long-polling loop:', 'async function qrLogin()', `// One systemd-supervised consumer owns the durable Redis processing list.
async function redisQueueLoop() {
  const configPath = process.env.QQ_REDIS_CONFIG_FILE || join(homedir(), '.config', 'qq-bridge', 'redis.json');
  const config = loadRedisConfig(configPath);
  await consumeRedisQueue({
    config,
    pollSecret: process.env.QQ_POLL_SECRET,
    handleEvent: processRedisEvent,
    onState(state, errorCode) {
      if (state === 'retry') {
        log('Redis reconnect scheduled:', errorCode);
        setStatus('reconnecting', { transport: 'redis-tls', app_id: process.env.QQBOT_APP_ID });
        return;
      }
      if (state !== 'idle') log('Redis queue:', state);
      setStatus('connected', {
        transport: 'redis-tls',
        app_id: process.env.QQBOT_APP_ID,
        last_poll_at: new Date().toISOString(),
      });
    },
  });
}

async function processRedisEvent(event) {
  const payload = JSON.parse(event.body);
  if (payload.op !== 0 || !payload.t) return;
  const dispatchEvent = await getDispatcher();
  const result = dispatchEvent(payload.t, payload.d, process.env.QQBOT_APP_ID || '');
  if (!result || result.action !== 'message' || !result.msg) return;
  const message = result.msg;
  if (!message.replyTarget) {
    if (message.kind === 'group' && message.groupOpenid) {
      message.replyTarget = { scope: 'group', targetId: message.groupOpenid, msgId: message.messageId };
    } else if (message.kind === 'c2c' && message.senderId) {
      message.replyTarget = { scope: 'c2c', targetId: message.senderId, msgId: message.messageId };
    }
  }
  // Errors propagate to the queue loop, retaining the encrypted processing item.
  await writeInbox(message);
}

`);
  updated = replaceOnce(updated,
    'const res = await fetch(url + "/api/qq-events?secret=" + encodeURIComponent(secret) + "&action=token");',
    `const res = await fetch(url + '/api/qq-events?action=token', {
      headers: { 'x-poll-secret': secret },
      signal: AbortSignal.timeout(15000),
    });`);
  updated = updated.replace('// TRANSPORT: webhook mode (2026-11-02).', '// TRANSPORT: webhook enqueue with direct VM Redis/TLS consumption.')
    .replace('-> pollWebhook()', '-> Redis processing list');
  return updated;
}

function install(target, expectedSha) {
  if (basename(target) !== 'bridge.mjs' || !/^[a-f0-9]{64}$/.test(expectedSha || '')) {
    throw new Error('usage_install_transport_bridge_path_expected_sha256');
  }
  const source = readFileSync(target, 'utf8');
  if (createHash('sha256').update(source).digest('hex') !== expectedSha) {
    throw new Error('bridge_sha_mismatch');
  }
  const updated = patchBridge(source);
  const temporary = join(dirname(target), '.bridge-redis-install.mjs');
  writeFileSync(temporary, updated, { mode: statSync(target).mode & 0o777 });
  execFileSync(process.execPath, ['--check', temporary], { stdio: 'pipe' });
  for (const name of ['redis-queue.mjs', 'durable-inbox.mjs']) {
    copyFileSync(new URL(name, import.meta.url), join(dirname(target), name));
  }
  renameSync(temporary, target);
  console.log(JSON.stringify({ installed: true, sha256: createHash('sha256').update(updated).digest('hex') }));
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  try { install(resolve(process.argv[2] || ''), process.argv[3]); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}

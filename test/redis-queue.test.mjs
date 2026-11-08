import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createCipheriv, createHash, randomBytes } from 'node:crypto';
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import { createRedisClient, decodeEvent, processNextEvent, QUEUE, PROCESSING, FAILED } from '../vm/redis-queue.mjs';
import { persistInbox } from '../vm/durable-inbox.mjs';
import { patchBridge } from '../vm/install-transport.mjs';

const secret = 'isolated-test-secret';
function encryptedEvent(id, content = 'hello') {
  const body = JSON.stringify({ op: 0, t: 'C2C_MESSAGE_CREATE', d: { id, content } });
  const iv = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', createHash('sha256').update(secret).digest(), iv);
  const data = Buffer.concat([cipher.update(body), cipher.final(), cipher.getAuthTag()]);
  return JSON.stringify({ id, uploadedAt: '2026-11-08T00:00:00Z', body: `${iv.toString('base64')}:${data.toString('base64')}` });
}

class QueueFixture {
  constructor(items = []) {
    this.queued = [...items];
    this.processing = [];
    this.invalid = [];
    this.calls = [];
  }
  async lindex(key, index) {
    assert.equal(key, PROCESSING);
    assert.equal(index, -1);
    return this.processing.at(-1) ?? null;
  }
  async blmove(source, destination, from, to, timeout) {
    this.calls.push('claim');
    assert.deepEqual([source, destination, from, to, timeout], [QUEUE, PROCESSING, 'RIGHT', 'LEFT', 25]);
    const raw = this.queued.pop() ?? null;
    if (raw !== null) this.processing.unshift(raw);
    return raw;
  }
  async lrem(key, count, raw) {
    this.calls.push('ack');
    assert.equal(key, PROCESSING);
    assert.equal(count, 1);
    const index = this.processing.indexOf(raw);
    if (index < 0) return 0;
    this.processing.splice(index, 1);
    return 1;
  }
  async eval(script, keys, source, destination, raw) {
    assert.match(script, /LREM/);
    assert.deepEqual([keys, source, destination], [2, PROCESSING, FAILED]);
    await this.lrem(source, 1, raw);
    this.invalid.unshift(raw);
  }
}

test('legacy AES envelopes decode without changing blank or Unicode messages', () => {
  for (const content of ['', '   ', '\u3000', '早上好🐟']) {
    const event = decodeEvent(encryptedEvent('message', content), secret);
    assert.equal(JSON.parse(event.body).d.content, content);
  }
});

test('FIFO delivery acknowledges only after the inbox callback succeeds', async () => {
  const client = new QueueFixture([encryptedEvent('second'), encryptedEvent('first')]);
  const delivered = [];
  for (let index = 0; index < 2; index++) {
    assert.equal(await processNextEvent(client, secret, async event => {
      assert.equal(client.processing.length, 1);
      delivered.push(event.id);
    }), 'delivered');
  }
  assert.deepEqual(delivered, ['first', 'second']);
  assert.deepEqual(client.calls, ['claim', 'ack', 'claim', 'ack']);
});

test('failed local handoff stays in processing and is recovered before new messages', async () => {
  const raw = encryptedEvent('recover');
  const client = new QueueFixture([encryptedEvent('later'), raw]);
  await assert.rejects(processNextEvent(client, secret, async () => { throw new Error('disk_full'); }), /disk_full/);
  assert.deepEqual(client.processing, [raw]);
  const delivered = [];
  await processNextEvent(client, secret, async event => delivered.push(event.id));
  assert.deepEqual(delivered, ['recover']);
  assert.equal(client.queued.length, 1);
  assert.equal(client.calls.filter(call => call === 'claim').length, 1);
});

test('uncertain Redis acknowledgement replays the same durable local filename once', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'qq-transport-'));
  try {
    mkdirSync(join(directory, 'processed'));
    const client = new QueueFixture([encryptedEvent('stable-id')]);
    const lrem = client.lrem.bind(client);
    client.lrem = async () => { throw new Error('connection_lost'); };
    const handle = async event => persistInbox(directory, event.id, JSON.parse(event.body));
    await assert.rejects(processNextEvent(client, secret, handle), /connection_lost/);
    const [name] = readdirSync(directory).filter(name => name.endsWith('.json'));
    renameSync(join(directory, name), join(directory, 'processed', name));
    client.lrem = lrem;
    await processNextEvent(client, secret, handle);
    assert.equal(readdirSync(directory).filter(name => name.endsWith('.json')).length, 0);
    assert.equal(JSON.parse(readFileSync(join(directory, 'processed', name))).d.id, 'stable-id');
    assert.equal(client.processing.length, 0);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test('invalid ciphertext is retained separately and does not block the next item', async () => {
  const client = new QueueFixture([encryptedEvent('valid'), '{bad json']);
  assert.equal(await processNextEvent(client, secret, async () => assert.fail()), 'invalid');
  assert.deepEqual(client.invalid, ['{bad json']);
  assert.equal(await processNextEvent(client, secret, async event => assert.equal(event.id, 'valid')), 'delivered');
});

test('idle wait leaves all queues untouched', async () => {
  const client = new QueueFixture();
  assert.equal(await processNextEvent(client, secret, async () => assert.fail()), 'idle');
  assert.equal(client.calls.includes('ack'), false);
});

test('native Redis always verifies TLS and disables implicit command resends', () => {
  const client = createRedisClient({ host: 'redis.example.test', password: 'fixture' }, {});
  try {
    assert.equal(client.options.tls.rejectUnauthorized, true);
    assert.equal(client.options.autoResendUnfulfilledCommands, false);
    assert.equal(client.options.enableOfflineQueue, false);
    assert.equal(client.options.commandTimeout, 35_000);
  } finally { client.disconnect(); }
});

test('a rejected proxy CONNECT fails without sending Redis credentials', async () => {
  let requestPath;
  const server = createServer();
  server.on('connect', (request, socket) => {
    requestPath = request.url;
    socket.end('HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n');
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const client = createRedisClient({ host: 'redis.example.test', password: 'fixture', proxyUrl: `http://127.0.0.1:${server.address().port}` }, {});
  try {
    await assert.rejects(client.connect(), /redis_proxy_connect_403/);
    assert.equal(requestPath, 'redis.example.test:6379');
  } finally {
    client.disconnect();
    await new Promise(resolve => server.close(resolve));
  }
});

test('the host patch preserves outgoing code while removing both HTTP poll paths', () => {
  const fixture = `import { promisify } from "util";
async function writeInbox(msg) {
  markSeen(msg.messageId);
  const file = {};
  const name = \`\${Date.now()}_\${msg.messageId.slice(-8)}.json\`;
  writeFileSync(join(INBOX, name), JSON.stringify(file, null, 2));
}
async function pollOutbox() { /* existing outgoing behavior */ }
async function pollWebhook() { /* legacy */ }
async function connectBot(appId) {
  log("Webhook poller READY (long-poll mode)");
  // Keep the process alive;
}
// Long-polling loop:
async function longPollLoop() { /* legacy */ }
async function qrLogin() {}
async function token() {
  const res = await fetch(url + "/api/qq-events?secret=" + encodeURIComponent(secret) + "&action=token");
}`;
  const patched = patchBridge(fixture);
  assert.match(patched, /existing outgoing behavior/);
  assert.doesNotMatch(patched, /function pollWebhook|function longPollLoop|\/api\/qq-wait/);
  assert.ok(patched.indexOf('persistInbox(INBOX') < patched.indexOf('markSeen(msg.messageId)'));
  assert.match(patched, /x-poll-secret/);
  assert.throws(() => patchBridge(patched), /bridge_patch_source_changed/);
});

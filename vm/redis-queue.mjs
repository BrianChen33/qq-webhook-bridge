import Redis, { AbstractConnector } from 'ioredis';
import http from 'node:http';
import https from 'node:https';
import tls from 'node:tls';
import { createDecipheriv, createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { setTimeout as sleep } from 'node:timers/promises';

export const QUEUE = 'qq:queue';
export const PROCESSING = 'qq:processing';
export const FAILED = 'qq:invalid';

// Use the host's existing permitted HTTP CONNECT proxy. Certificate validation
// remains enabled for both an HTTPS proxy and the Redis TLS connection.
export class ProxyConnector extends AbstractConnector {
  constructor(options) {
    super(options.disconnectTimeout);
    this.options = options;
  }

  connect() {
    this.connecting = true;
    const { host, port, proxyUrl, connectTimeout } = this.options;
    const proxy = new URL(proxyUrl);
    const transport = proxy.protocol === 'https:' ? https : http;
    return new Promise((resolve, reject) => {
      const headers = { Host: `${host}:${port}` };
      if (proxy.username || proxy.password) {
        const credentials = `${decodeURIComponent(proxy.username)}:${decodeURIComponent(proxy.password)}`;
        headers['Proxy-Authorization'] = `Basic ${Buffer.from(credentials).toString('base64')}`;
      }
      const request = transport.request({
        hostname: proxy.hostname,
        port: proxy.port || (proxy.protocol === 'https:' ? 443 : 80),
        method: 'CONNECT',
        path: `${host}:${port}`,
        headers,
        agent: false,
      });
      this.request = request;
      const deadline = setTimeout(() => request.destroy(new Error('redis_proxy_timeout')), connectTimeout);
      request.once('error', error => { clearTimeout(deadline); reject(error); });
      request.once('connect', (response, socket, head) => {
        clearTimeout(deadline);
        if (response.statusCode !== 200 || !this.connecting) {
          socket.destroy();
          reject(new Error(`redis_proxy_connect_${response.statusCode}`));
          return;
        }
        if (head.length) socket.unshift(head);
        this.stream = tls.connect({ socket, servername: host, rejectUnauthorized: true });
        const stream = this.stream;
        stream.setTimeout(connectTimeout, () => stream.destroy(new Error('redis_tls_timeout')));
        stream.once('secureConnect', () => stream.setTimeout(0));
        this.stream.once('error', error => { this.firstError = error; });
        // ioredis attaches its own secureConnect, timeout and error listeners.
        resolve(this.stream);
      });
      request.end();
    });
  }

  disconnect() {
    this.request?.destroy();
    super.disconnect();
  }
}

export function createRedisClient(config, env = process.env) {
  if (!config?.host || !config.password) throw new Error('redis_config_missing');
  const proxyUrl = config.proxyUrl ?? env.HTTPS_PROXY ?? env.https_proxy ?? env.HTTP_PROXY ?? env.http_proxy;
  if (proxyUrl && !['http:', 'https:'].includes(new URL(proxyUrl).protocol)) {
    throw new Error('redis_proxy_protocol_unsupported');
  }
  const client = new Redis({
    host: config.host,
    port: config.port || 6379,
    username: config.username || 'default',
    password: config.password,
    tls: { servername: config.host, rejectUnauthorized: true },
    ...(proxyUrl ? { Connector: ProxyConnector, proxyUrl } : {}),
    lazyConnect: true,
    enableReadyCheck: false,
    enableOfflineQueue: false,
    autoResendUnfulfilledCommands: false,
    retryStrategy: null,
    maxRetriesPerRequest: 0,
    connectTimeout: 10_000,
    commandTimeout: 35_000,
    disconnectTimeout: 1_000,
    disableClientInfo: true,
    protocol: 2,
  });
  // Errors are reported by the bounded consumer loop without credential-bearing
  // command arguments or decrypted message bodies.
  client.on('error', () => {});
  return client;
}

export function loadRedisConfig(path) {
  return JSON.parse(readFileSync(path, 'utf8'));
}

export function decodeEvent(raw, pollSecret) {
  if (!pollSecret) throw new Error('poll_secret_missing');
  const envelope = JSON.parse(raw);
  if (typeof envelope.id !== 'string' || typeof envelope.body !== 'string') {
    throw new Error('invalid_queue_envelope');
  }
  const separator = envelope.body.indexOf(':');
  if (separator < 1) throw new Error('invalid_queue_ciphertext');
  const iv = Buffer.from(envelope.body.slice(0, separator), 'base64');
  const data = Buffer.from(envelope.body.slice(separator + 1), 'base64');
  if (iv.length !== 12 || data.length < 16) throw new Error('invalid_queue_ciphertext');
  const key = createHash('sha256').update(pollSecret, 'utf8').digest();
  const decipher = createDecipheriv('aes-256-gcm', key, iv);
  decipher.setAuthTag(data.subarray(-16));
  const body = Buffer.concat([decipher.update(data.subarray(0, -16)), decipher.final()]).toString('utf8');
  const payload = JSON.parse(body);
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    throw new Error('invalid_queue_payload');
  }
  return { id: envelope.id, uploadedAt: envelope.uploadedAt, url: null, body };
}

export async function processNextEvent(client, pollSecret, handleEvent) {
  // Recover the oldest unacknowledged event first, including a BLMOVE whose
  // response was lost during a disconnect. Only one supervised consumer runs.
  const pending = await client.lindex(PROCESSING, -1);
  const raw = pending ?? await client.blmove(QUEUE, PROCESSING, 'RIGHT', 'LEFT', 25);
  if (raw === null) return 'idle';
  let event;
  try {
    event = decodeEvent(raw, pollSecret);
  } catch {
    // Keep the original ciphertext for diagnosis. A malformed event must not
    // hold up later messages, and removal/movement must be a single transaction.
    await client.eval(`
      if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 1 then
        redis.call('LPUSH', KEYS[2], ARGV[1])
        return 1
      end
      return 0
    `, 2, PROCESSING, FAILED, raw);
    return 'invalid';
  }
  await handleEvent(event);
  await client.lrem(PROCESSING, 1, raw);
  return 'delivered';
}

export async function consumeRedisQueue({ config, pollSecret, handleEvent, onState = () => {}, signal }) {
  if (!pollSecret) throw new Error('poll_secret_missing');
  while (!signal?.aborted) {
    const client = createRedisClient(config);
    try {
      await client.connect();
      await client.ping();
      onState('ready');
      while (!signal?.aborted) {
        onState(await processNextEvent(client, pollSecret, handleEvent));
      }
    } catch (error) {
      onState('retry', error.code || error.name);
    } finally {
      client.disconnect();
    }
    if (!signal?.aborted) await sleep(5_000, undefined, { signal });
  }
}

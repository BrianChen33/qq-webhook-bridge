import { redisCmd } from './_redis.js';
import { createDecipheriv, createHash } from 'crypto';

// Poller endpoint (Upstash Redis version). Replaces Vercel Blob version.
// Kept for backward compat; bridge now uses /api/qq-wait for long-polling.
//
//   GET  /api/qq-events?secret=...            -> { events: [...] } (drains queue)
//   GET  /api/qq-events?secret=...&action=token -> { access_token } (QQ token via Vercel secret)
//   DELETE /api/qq-events?secret=...          -> { ok: true } (no-op, queue already drained on GET)

function encKey() {
  return createHash('sha256').update(String(process.env.POLL_SECRET || ''), 'utf8').digest();
}
function decrypt(payload) {
  const i = payload.indexOf(':');
  const iv = Buffer.from(payload.slice(0, i), 'base64');
  const data = Buffer.from(payload.slice(i + 1), 'base64');
  const tag = data.subarray(data.length - 16);
  const ct = data.subarray(0, data.length - 16);
  const d = createDecipheriv('aes-256-gcm', encKey(), iv);
  d.setAuthTag(tag);
  return Buffer.concat([d.update(ct), d.final()]).toString('utf8');
}

let tokenCache = { token: null, expiresAt: 0 };
async function getQQToken() {
  const now = Date.now();
  if (tokenCache.token && tokenCache.expiresAt > now + 60000) {
    return tokenCache.token;
  }
  const appId = '1906084954';
  const clientSecret = process.env.QQ_CLIENT_SECRET;
  if (!clientSecret) throw new Error('not_configured');
  const res = await fetch('https://bots.qq.com/app/getAppAccessToken', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ appId, clientSecret }),
  });
  const data = await res.json();
  if (!data.access_token) throw new Error('token_failed: ' + JSON.stringify(data).slice(0, 100));
  tokenCache = {
    token: data.access_token,
    expiresAt: now + (parseInt(data.expires_in || '7200', 10) * 1000),
  };
  return tokenCache.token;
}

export default async function handler(req, res) {
  const expected = process.env.POLL_SECRET;
  const provided = req.query.secret || req.headers['x-poll-secret'];
  if (!expected || provided !== expected) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  if (req.method === 'GET') {
    if (req.query.action === 'token') {
      try {
        const token = await getQQToken();
        return res.status(200).json({ access_token: token });
      } catch (e) {
        return res.status(500).json({ error: String(e.message).slice(0, 200) });
      }
    }
    // Drain the queue (for backward compat; qq-wait is preferred)
    const items = await redisCmd('LRANGE', 'qq:queue', '0', '-1');
    if (items && items.length > 0) {
      await redisCmd('DEL', 'qq:queue');
    }
    const seq = parseInt((await redisCmd('GET', 'qq:seq')) || '0', 10) || 0;
    const events = [];
    for (const item of items || []) {
      try {
        const parsed = JSON.parse(item);
        events.push({
          id: parsed.id,
          uploadedAt: parsed.uploadedAt,
          url: null,
          body: decrypt(parsed.body),
        });
      } catch {}
    }
    events.sort((a, b) => (a.uploadedAt < b.uploadedAt ? -1 : 1));
    return res.status(200).json({ events, seq });
  }

  if (req.method === 'DELETE') {
    // No-op: queue is drained on GET. Kept for backward compat.
    return res.status(200).json({ ok: true });
  }

  res.setHeader('Allow', 'GET, DELETE');
  return res.status(405).end();
}

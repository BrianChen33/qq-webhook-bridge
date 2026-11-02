import { redisCmd } from './_redis.js';
import { createDecipheriv, createHash } from 'crypto';

// Long-polling endpoint for the bridge (Upstash Redis version, BLPOP-based).
//
// Uses Redis BLPOP (blocking pop) instead of sleep-loop polling:
//   - Idle: 1 BLPOP per 25s = ~3,456 commands/day (well under 10k/day free tier)
//   - Old sleep-loop design: ~26 GETs per 25s = ~90k/day (WOULD exceed quota)
//
// Query params:
//   secret=<POLL_SECRET>  (required)
//   timeout=<seconds>     (optional, max 25, default 25)
//
// Response: { events: [...], seq: <latest seq>, timeout: true/false }

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

async function drainQueue() {
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
  return { events, seq };
}

export default async function handler(req, res) {
  const expected = process.env.POLL_SECRET;
  const provided = req.query.secret || req.headers['x-poll-secret'];
  if (!expected || provided !== expected) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).end();
  }

  const timeoutSec = Math.min(parseInt(req.query.timeout || '25', 10) || 25, 25);

  // BLPOP blocks up to timeoutSec waiting for an item on qq:queue.
  // Returns [key, value] if item arrives, null on timeout.
  // This is 1 Redis command per 25s when idle (vs 26 with sleep-loop).
  try {
    const result = await redisCmd('BLPOP', 'qq:queue', String(timeoutSec));
    if (result && result.length === 2) {
      // Got one event via BLPOP (already popped). Drain any others that arrived.
      const firstItem = result[1];
      const rest = await redisCmd('LRANGE', 'qq:queue', '0', '-1');
      if (rest && rest.length > 0) {
        await redisCmd('DEL', 'qq:queue');
      }
      const seq = parseInt((await redisCmd('GET', 'qq:seq')) || '0', 10) || 0;
      const allItems = [firstItem, ...(rest || [])];
      const events = [];
      for (const item of allItems) {
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
      return res.status(200).json({ events, seq, timeout: false });
    }
  } catch (e) {
    // BLPOP failed — fall back to immediate check
    console.warn('[qq-wait] BLPOP failed:', e.message);
  }

  // Timeout or error: check once more before returning empty
  const seq = parseInt((await redisCmd('GET', 'qq:seq').catch(() => '0')) || '0', 10) || 0;
  return res.status(200).json({ events: [], seq, timeout: true });
}

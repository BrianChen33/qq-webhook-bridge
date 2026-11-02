import { redisCmd, redisPipeline } from './_redis.js';
import nacl from 'tweetnacl';
import { createCipheriv, randomBytes, createHash } from 'crypto';

// QQ requires the raw body for Ed25519 verification.
export const config = { api: { bodyParser: false } };

// AES-256-GCM encryption for queued events (store is public-writable only,
// so payloads are encrypted; the poller endpoint decrypts server-side).
function encKey() {
  return createHash('sha256').update(String(process.env.POLL_SECRET || ''), 'utf8').digest();
}
function encrypt(plain) {
  const iv = randomBytes(12);
  const c = createCipheriv('aes-256-gcm', encKey(), iv);
  const ct = Buffer.concat([c.update(plain, 'utf8'), c.final(), c.getAuthTag()]);
  return iv.toString('base64') + ':' + ct.toString('base64');
}

// Derive the ed25519 keypair from the bot secret, per QQ's official doc:
// seed = secret repeated/truncated to 32 bytes, then GenerateKey(seed).
function keypairFromSecret(secret) {
  let seed = secret;
  while (seed.length < 32) seed += seed;
  seed = seed.slice(0, 32);
  return nacl.sign.keyPair.fromSeed(Buffer.from(seed, 'utf8'));
}

async function readRawBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  return Buffer.concat(chunks);
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).end();
  }

  const secret = process.env.QQ_CLIENT_SECRET;
  if (!secret) return res.status(500).json({ error: 'not_configured' });

  let rawBody;
  try {
    rawBody = await readRawBody(req);
  } catch {
    return res.status(400).end();
  }

  // DEBUG logging removed (was burning Advanced Operations on every POST).
  // To re-enable temporarily, uncomment the block below.
  /*
  try {
    const dbg = {
      ts: new Date().toISOString(),
      headers: {
        'user-agent': req.headers['user-agent'],
        'x-bot-appid': req.headers['x-bot-appid'],
        'x-signature-ed25519': req.headers['x-signature-ed25519'] ? 'present' : 'absent',
        'x-signature-timestamp': req.headers['x-signature-timestamp'],
      },
      bodyPreview: rawBody.toString('utf8').slice(0, 500),
    };
    await put(`qq-debug/${Date.now()}.json`, JSON.stringify(dbg), {
      access: 'public', contentType: 'application/json', addRandomSuffix: false,
    });
  } catch {}
  */

  let payload;
  try {
    payload = JSON.parse(rawBody.toString('utf8'));
  } catch {
    return res.status(400).end();
  }

  // op 13: QQ verifying our callback URL. Respond with signed plain_token.
  if (payload && payload.op === 13) {
    const d = payload.d || {};
    const plainToken = d.plain_token;
    const eventTs = d.event_ts;
    if (!plainToken || !eventTs) return res.status(400).end();
    const kp = keypairFromSecret(secret);
    const msg = Buffer.from(String(eventTs) + String(plainToken), 'utf8');
    const signature = Buffer.from(nacl.sign.detached(msg, kp.secretKey)).toString('hex');
    return res.status(200).json({ plain_token: plainToken, signature });
  }

  // Regular event: verify Ed25519 signature over timestamp + raw body.
  const sigHex = req.headers['x-signature-ed25519'];
  const timestamp = req.headers['x-signature-timestamp'];
  if (!sigHex || !timestamp) return res.status(401).end();
  let sig;
  try {
    sig = Buffer.from(String(sigHex), 'hex');
    if (sig.length !== nacl.sign.signatureLength) throw new Error('bad sig length');
  } catch {
    return res.status(401).end();
  }
  const kp = keypairFromSecret(secret);
  const msg = Buffer.concat([Buffer.from(String(timestamp), 'utf8'), rawBody]);
  if (!nacl.sign.detached.verify(msg, sig, kp.publicKey)) {
    return res.status(401).end();
  }

  // Queue the verified event in Upstash Redis (replaces Vercel Blob - quota exhausted).
  // Dedup: SISMEMBER on qq:seen set. Queue: LPUSH to qq:queue. Seq: INCR qq:seq.
  const qqEventId = (payload.d && payload.d.id) ? String(payload.d.id) : null;
  const eventKey = qqEventId ? qqEventId.replace(/[^a-zA-Z0-9._-]/g, '_') : `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  const encrypted = encrypt(rawBody.toString('utf8'));
  const queueItem = JSON.stringify({ id: eventKey, uploadedAt: new Date().toISOString(), body: encrypted });
  try {
    if (qqEventId) {
      const isDup = await redisCmd('SISMEMBER', 'qq:seen', eventKey);
      if (isDup === 1) {
        return res.status(200).json({ ok: true, deduped: true });
      }
    }
    // Atomic: add to seen set, push to queue, bump seq, set expiry
    await redisPipeline([
      ['SADD', 'qq:seen', eventKey],
      ['EXPIRE', 'qq:seen', '604800'],
      ['LPUSH', 'qq:queue', queueItem],
      ['INCR', 'qq:seq'],
    ]);
  } catch (e) {
    return res.status(500).json({ error: 'queue_failed', detail: String(e && e.message || e).slice(0, 200) });
  }
  return res.status(200).json({ ok: true });
}

// Shared Upstash Redis client for QQ webhook functions.
// Uses REST API with Bearer token from env (UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN).
// Falls back to pipeline for atomic multi-command ops.

const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

if (!REDIS_URL || !REDIS_TOKEN) {
  console.warn('[redis] UPSTASH_REDIS_REST_URL/TOKEN not configured');
}

async function redisCmd(...args) {
  const url = `${REDIS_URL}/${args.map(encodeURIComponent).join('/')}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${REDIS_TOKEN}` },
  });
  if (!res.ok) throw new Error(`redis ${args[0]} failed: HTTP ${res.status}`);
  const data = await res.json();
  return data.result;
}

async function redisPipeline(commands) {
  const res = await fetch(`${REDIS_URL}/pipeline`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${REDIS_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(commands),
  });
  if (!res.ok) throw new Error(`redis pipeline failed: HTTP ${res.status}`);
  const data = await res.json();
  return data.map((d) => d.result);
}

export { redisCmd, redisPipeline, REDIS_URL };

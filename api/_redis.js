// Shared Upstash Redis client for QQ webhook functions.
// Uses REST API with Bearer token from env (UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN).
// Pipelines are not transactions; use EVAL for operations requiring atomicity.

const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

if (!REDIS_URL || !REDIS_TOKEN) {
  console.warn('[redis] UPSTASH_REDIS_REST_URL/TOKEN not configured');
}

async function redisCmd(...args) {
  if (!REDIS_URL || !REDIS_TOKEN) throw new Error('redis_not_configured');
  const url = `${REDIS_URL}/${args.map(encodeURIComponent).join('/')}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${REDIS_TOKEN}` },
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`redis ${args[0]} failed: HTTP ${res.status}`);
  const data = await res.json();
  if (data.error) throw new Error(`redis ${args[0]}: ${data.error}`);
  return data.result;
}

async function redisPipeline(commands) {
  if (!REDIS_URL || !REDIS_TOKEN) throw new Error('redis_not_configured');
  const res = await fetch(`${REDIS_URL}/pipeline`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${REDIS_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(commands),
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`redis pipeline failed: HTTP ${res.status}`);
  const data = await res.json();
  if (!Array.isArray(data)) throw new Error('redis pipeline: invalid response');
  const failed = data.find(d => d.error);
  if (failed) throw new Error(`redis pipeline: ${failed.error}`);
  return data.map((d) => d.result);
}

export { redisCmd, redisPipeline, REDIS_URL };

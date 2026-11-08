// Authenticated short-lived QQ token proxy. Queue consumption runs on the VM.
let tokenCache = { token: null, expiresAt: 0 };

async function getQQToken() {
  const now = Date.now();
  if (tokenCache.token && tokenCache.expiresAt > now + 60000) return tokenCache.token;
  const clientSecret = process.env.QQ_CLIENT_SECRET;
  if (!clientSecret) throw new Error('not_configured');
  const response = await fetch('https://bots.qq.com/app/getAppAccessToken', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ appId: '1906084954', clientSecret }),
    signal: AbortSignal.timeout(10000),
  });
  if (!response.ok) throw new Error(`token_http_${response.status}`);
  const data = await response.json();
  if (!data.access_token) throw new Error('token_failed');
  tokenCache = {
    token: data.access_token,
    expiresAt: now + (parseInt(data.expires_in || '7200', 10) * 1000),
  };
  return tokenCache.token;
}

export default async function handler(req, res) {
  const expected = process.env.POLL_SECRET;
  const provided = req.query.secret || req.headers['x-poll-secret'];
  res.setHeader('Cache-Control', 'no-store');
  if (!expected || provided !== expected) return res.status(401).json({ error: 'unauthorized' });
  if (req.method === 'GET' && req.query.action === 'token') {
    try {
      return res.status(200).json({ access_token: await getQQToken() });
    } catch (error) {
      return res.status(502).json({ error: error.name === 'TimeoutError' ? 'token_timeout' : 'token_unavailable' });
    }
  }
  if (req.method === 'GET' || req.method === 'DELETE') {
    return res.status(410).json({ error: 'polling_retired', transport: 'direct_redis_tls' });
  }
  res.setHeader('Allow', 'GET');
  return res.status(405).end();
}

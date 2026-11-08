// Legacy polling is retired. Waiting belongs to the VM's direct Redis consumer.
export default function handler(req, res) {
  const expected = process.env.POLL_SECRET;
  const provided = req.query.secret || req.headers['x-poll-secret'];
  res.setHeader('Cache-Control', 'no-store');
  if (!expected || provided !== expected) return res.status(401).json({ error: 'unauthorized' });
  return res.status(410).json({ error: 'polling_retired', transport: 'direct_redis_tls' });
}

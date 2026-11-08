import { createRedisClient, loadRedisConfig, QUEUE, PROCESSING } from './redis-queue.mjs';

const client = createRedisClient(loadRedisConfig(process.argv[2]));
try {
  await client.connect();
  const ping = await client.ping();
  const queued = await client.llen(QUEUE);
  const processing = await client.llen(PROCESSING);
  console.log(JSON.stringify({ ping, queued, processing, tls: client.stream.authorized === true }));
} catch (error) {
  console.error(JSON.stringify({ error: error.code || error.name }));
  process.exitCode = 1;
} finally {
  client.disconnect();
}

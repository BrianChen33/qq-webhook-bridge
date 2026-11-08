import { test, before, beforeEach, after } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { Readable } from 'node:stream';
import nacl from 'tweetnacl';

let mode = 'ok';
const commands = [];
const server = createServer(async (req, res) => {
  let body = ''; for await (const c of req) body += c;
  const args = req.url === '/' ? JSON.parse(body) : decodeURIComponent(req.url.slice(1)).split('/');
  commands.push(args);
  res.setHeader('Content-Type', 'application/json');
  if (mode === 'error') return res.end(JSON.stringify({ error: 'ERR test backend failure' }));
  if (req.url === '/pipeline') return res.end(JSON.stringify([{error:'ERR test pipeline failure'}]));
  const result = args[0] === 'GET' ? '0' : args[0] === 'EVAL' ? 1 : null;
  res.end(JSON.stringify({result}));
});
let redis, wait, webhook, events;
before(async () => {
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  process.env.UPSTASH_REDIS_REST_URL = `http://127.0.0.1:${server.address().port}`;
  process.env.UPSTASH_REDIS_REST_TOKEN = 'test-token';
  process.env.POLL_SECRET = 'test-poll-secret';
  process.env.QQ_CLIENT_SECRET = 'test-bot-secret';
  redis = await import('../api/_redis.js');
  wait = (await import('../api/qq-wait.js')).default;
  webhook = (await import('../api/qq-webhook.js')).default;
  events = (await import('../api/qq-events.js')).default;
});
after(() => server.close());
beforeEach(() => { mode='ok'; });
function response() {return {code:200, headers:{},setHeader(k,v){this.headers[k]=v;},status(n){this.code=n;return this;},json(v){this.body=v;return this;},end(){return this;}};}
function signedRequest(payload) {
  const body = JSON.stringify(payload), ts = '1793640000';
  const seed = Buffer.from('test-bot-secret'.repeat(3).slice(0,32));
  const kp = nacl.sign.keyPair.fromSeed(seed);
  const req = Readable.from([Buffer.from(body)]);
  req.method = 'POST'; req.headers = {'x-signature-timestamp':ts,'x-signature-ed25519':Buffer.from(nacl.sign.detached(Buffer.from(ts+body),kp.secretKey)).toString('hex')};
  return req;
}
test('Redis errors cannot look like an empty queue', async () => {
  mode='error'; await assert.rejects(redis.redisCmd('GET','qq:seq'), /backend failure/); mode='ok';
});
test('pipeline command errors are propagated', async () => {
  await assert.rejects(redis.redisPipeline([['GET','x']]), /pipeline failure/);
});
test('retired wait route returns immediately without accessing Redis', async () => {
  mode='error'; commands.length=0; const res=response();
  await wait({method:'GET',query:{timeout:'1'},headers:{'x-poll-secret':'test-poll-secret'}},res);
  mode='ok'; assert.equal(res.code,410); assert.equal(commands.length,0);
});

test('legacy drain and delete routes cannot consume or acknowledge queue items', async () => {
  commands.length=0;
  for (const method of ['GET', 'DELETE']) {
    const res=response();
    await events({method,query:{},headers:{'x-poll-secret':'test-poll-secret'}},res);
    assert.equal(res.code,410);
  }
  assert.equal(commands.length,0);
});
test('verified QQ dispatch receives the protocol ACK after durable enqueue', async () => {
  commands.length=0; const res=response();
  await webhook(signedRequest({id:'event-1',op:0,t:'C2C_MESSAGE_CREATE',d:{id:'message-1',content:'test'}}),res);
  assert.equal(res.code,200); assert.deepEqual(res.body,{op:12});
  assert.ok(commands.some(c=>c[0]==='EVAL'),'dedupe and enqueue must be atomic');
});
test('unsigned events cannot reach the queue', async () => {
  commands.length=0; const req=Readable.from([Buffer.from('{"op":0}')]); req.method='POST'; req.headers={};
  const res=response(); await webhook(req,res); assert.equal(res.code,401); assert.equal(commands.length,0);
});

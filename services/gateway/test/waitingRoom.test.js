// Runs against a real Redis when TEST_REDIS_URL is set, e.g.
//   TEST_REDIS_URL=redis://localhost:6379/15 npm test
import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';

import Redis from 'ioredis';

import { WaitingRoom } from '../src/lib/waitingRoom.js';

const url = process.env.TEST_REDIS_URL;
let redis;

before(async () => {
  if (!url) return;
  redis = new Redis(url);
  await redis.flushdb();
});
after(() => redis?.disconnect());

const room = (opts = {}) =>
  new WaitingRoom({
    redis,
    queueSecret: 's',
    admissionTtlSec: 60,
    defaultRate: 3,
    instanceId: opts.instanceId || 'gw-1',
    upstreamHealth: opts.health || (() => 'closed'),
  });

test('join is idempotent and FIFO under a concurrent stampede', { skip: !url }, async () => {
  const wr = room();
  const users = Array.from({ length: 500 }, (_, i) => `u${i}`);
  // Everyone joins at once, and everyone double-clicks.
  const results = await Promise.all([...users, ...users].map((u) => wr.join(u, '42').then((r) => ({ u, r }))));
  const seqByUser = new Map();
  for (const { u, r } of results) {
    const seq = JSON.parse(Buffer.from(r.ticket.split('.')[1], 'base64url')).seq;
    if (seqByUser.has(u)) assert.equal(seqByUser.get(u), seq, 'double join must keep the same position');
    seqByUser.set(u, seq);
  }
  const seqs = [...seqByUser.values()].sort((a, b) => a - b);
  assert.deepEqual(seqs, Array.from({ length: 500 }, (_, i) => i + 1), 'positions are unique and gap-free');
});

test('admitter advances at the configured rate and issues admission tokens', { skip: !url }, async () => {
  const wr = room();
  const t1 = (await wr.join('alice', '7')).ticket;
  const t5 = await Promise.all(['b', 'c', 'd', 'e'].map((u) => wr.join(u, '7')));
  assert.equal((await wr.status('alice', '7', t1)).state, 'waiting');

  await wr.tick(); // admits 3
  wr.watermarks.clear();
  const s = await wr.status('alice', '7', t1);
  assert.equal(s.state, 'admitted');
  assert.ok(wr.isAdmitted('alice', '7', s.admission_token));
  assert.equal(wr.isAdmitted('mallory', '7', s.admission_token), false, 'token bound to user');
  assert.equal(wr.isAdmitted('alice', '8', s.admission_token), false, 'token bound to event');

  const last = await wr.status('e', '7', t5[3].ticket);
  assert.equal(last.state, 'waiting');
  assert.equal(last.position, 2);
});

test('admission pauses while the booking service breaker is open (backpressure)', { skip: !url }, async () => {
  let health = 'open';
  const wr = room({ health: () => health });
  await wr.join('x1', '9');
  await wr.tick();
  assert.equal(await redis.get('wr:{9}:admitted'), null, 'nobody admitted while upstream is down');
  health = 'closed';
  await wr.tick();
  assert.equal(await redis.get('wr:{9}:admitted'), '1');
});

test('only one gateway instance is the admitter', { skip: !url }, async () => {
  await redis.del('wr:admitter:leader');
  const a = room({ instanceId: 'a' });
  const b = room({ instanceId: 'b' });
  await Promise.all([a.startAdmitter({ intervalMs: 50 }), b.startAdmitter({ intervalMs: 50 })]);
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(Number(a.isLeader) + Number(b.isLeader), 1);
  a.stopAdmitter();
  b.stopAdmitter();
});

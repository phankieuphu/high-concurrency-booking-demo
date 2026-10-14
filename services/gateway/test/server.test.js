import assert from 'node:assert/strict';
import { test } from 'node:test';

import { loadConfig } from '../src/config.js';
import { LocalTokenBucket } from '../src/lib/rateLimiter.js';
import { issueDevUserToken } from '../src/lib/tokens.js';
import { UpstreamError } from '../src/lib/upstream.js';
import { buildApp } from '../src/server.js';

const config = { ...loadConfig({}), maxInFlight: 100 };
const auth = (u = 'alice') => ({ authorization: `Bearer ${issueDevUserToken(u, config.jwtSecret)}` });

function make({ upstreamImpl, admitted = true, capacity = 100 } = {}) {
  const calls = [];
  const upstream = {
    async request(method, path, opts = {}) {
      calls.push({ method, path, opts });
      if (upstreamImpl) return upstreamImpl(method, path, opts);
      if (path.startsWith('/v1/events/')) return { status: 200, body: { id: 1, waiting_room: true }, headers: {} };
      return { status: 201, body: { id: 'r1', status: 'HELD' }, headers: {} };
    },
  };
  const bucket = new LocalTokenBucket({ capacity, refillPerSec: 0.0001 });
  const rateLimiter = { take: async (k, c) => bucket.take(k, c) };
  const waitingRoom = { isAdmitted: () => admitted };
  const app = buildApp({ config, upstream, rateLimiter, waitingRoom, logger: false });
  return { app, calls };
}

const reserve = (app, headers = {}) =>
  app.inject({
    method: 'POST',
    url: '/api/v1/reservations',
    headers: { ...auth(), 'idempotency-key': 'k1', 'x-admission-token': 't', ...headers },
    payload: { event_id: 1, tier_id: 2, quantity: 1 },
  });

test('rejects unauthenticated requests', async () => {
  const { app } = make();
  const res = await reserve(app, { authorization: 'Bearer nope' });
  assert.equal(res.statusCode, 401);
});

test('requires an idempotency key', async () => {
  const { app } = make();
  const res = await reserve(app, { 'idempotency-key': '' });
  assert.equal(res.statusCode, 400);
});

test('blocks reservations for waiting-room events without admission', async () => {
  const { app, calls } = make({ admitted: false });
  const res = await reserve(app);
  assert.equal(res.statusCode, 403);
  assert.ok(!calls.some((c) => c.path === '/v1/reservations'), 'never reaches booking service');
});

test('forwards admitted reservations with user id and idempotency key', async () => {
  const { app, calls } = make();
  const res = await reserve(app);
  assert.equal(res.statusCode, 201);
  const call = calls.find((c) => c.path === '/v1/reservations');
  assert.equal(call.opts.headers['x-user-id'], 'alice');
  assert.equal(call.opts.headers['idempotency-key'], 'k1');
});

test('rate limits per user with 429 + Retry-After', async () => {
  const { app } = make({ capacity: 2 });
  const codes = [];
  for (let i = 0; i < 4; i++) codes.push((await reserve(app)).statusCode);
  assert.deepEqual(codes, [201, 201, 429, 429]);
});

test('maps upstream outages to 503 with Retry-After', async () => {
  const { app } = make({
    upstreamImpl: (m, path) => {
      if (path.startsWith('/v1/events/')) return { status: 200, body: { waiting_room: false }, headers: {} };
      throw new UpstreamError('booking service temporarily unavailable', { status: 503, retryAfter: 5 });
    },
  });
  const res = await reserve(app);
  assert.equal(res.statusCode, 503);
  assert.equal(res.headers['retry-after'], '5');
});

test('collapses concurrent catalog reads into one upstream call', async () => {
  let n = 0;
  const { app } = make({
    upstreamImpl: async () => {
      n += 1;
      await new Promise((r) => setTimeout(r, 20));
      return { status: 200, body: { id: 5 }, headers: {} };
    },
  });
  const res = await Promise.all(Array.from({ length: 50 }, () => app.inject({ method: 'GET', url: '/api/v1/events/5' })));
  assert.ok(res.every((r) => r.statusCode === 200));
  assert.equal(n, 1);
});

test('sheds load above the in-flight limit', async () => {
  let release;
  const gate = new Promise((r) => (release = r));
  const upstream = {
    async request() {
      await gate;
      return { status: 200, body: [], headers: {} };
    },
  };
  const app = buildApp({
    config: { ...config, maxInFlight: 2 },
    upstream,
    rateLimiter: { take: async () => ({ allowed: true, remaining: 1 }) },
    waitingRoom: {},
    logger: false,
  });
  const slow = [app.inject({ url: '/api/v1/events' }), app.inject({ url: '/api/v1/events/1' })];
  await new Promise((r) => setTimeout(r, 20));
  const third = await app.inject({ url: '/api/v1/events/2' });
  assert.equal(third.statusCode, 503);
  release();
  await Promise.all(slow);
  const after = await app.inject({ url: '/api/v1/events/3' });
  assert.equal(after.statusCode, 200);
});

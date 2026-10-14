import assert from 'node:assert/strict';
import { test } from 'node:test';

import { CircuitBreaker, CircuitOpenError } from '../src/lib/circuitBreaker.js';
import { LocalTokenBucket, RateLimiter } from '../src/lib/rateLimiter.js';
import { Upstream } from '../src/lib/upstream.js';

test('circuit breaker opens after threshold and recovers via half-open probe', async () => {
  let now = 0;
  const b = new CircuitBreaker({ name: 't', threshold: 2, cooldownMs: 1000, now: () => now });
  const boom = () => Promise.reject(new Error('boom'));
  await assert.rejects(b.exec(boom));
  await assert.rejects(b.exec(boom));
  assert.equal(b.state, 'open');
  await assert.rejects(b.exec(async () => 'ok'), CircuitOpenError);

  now = 1500;
  assert.equal(await b.exec(async () => 'ok'), 'ok');
  assert.equal(b.state, 'closed');
});

test('local token bucket enforces burst and refills over time', () => {
  let now = 0;
  const bucket = new LocalTokenBucket({ capacity: 3, refillPerSec: 1, now: () => now });
  assert.ok(bucket.take('u').allowed);
  assert.ok(bucket.take('u').allowed);
  assert.ok(bucket.take('u').allowed);
  assert.equal(bucket.take('u').allowed, false);
  now = 1000;
  assert.ok(bucket.take('u').allowed);
  assert.ok(bucket.take('other').allowed, 'buckets are per key');
});

test('rate limiter falls back to local buckets when Redis fails', async () => {
  const brokenRedis = {
    defineCommand() {},
    tokenBucket: () => Promise.reject(new Error('ECONNREFUSED')),
  };
  const rl = new RateLimiter({
    redis: brokenRedis,
    breaker: new CircuitBreaker({ name: 'redis', threshold: 1 }),
    capacity: 2,
    refillPerSec: 0.001,
  });
  const results = [await rl.take('u'), await rl.take('u'), await rl.take('u')];
  assert.deepEqual(results.map((r) => r.allowed), [true, true, false]);
  assert.ok(results.every((r) => r.source === 'local'));
});

test('upstream retries connection errors once, then trips the breaker', async () => {
  let calls = 0;
  const fetchImpl = async () => {
    calls += 1;
    throw new TypeError('fetch failed');
  };
  const breaker = new CircuitBreaker({ name: 'booking', threshold: 4, cooldownMs: 60_000 });
  const up = new Upstream({ baseUrl: 'http://x', timeoutMs: 100, breaker, fetchImpl });
  await assert.rejects(up.request('GET', '/v1/events'), (e) => e.status === 503);
  assert.equal(calls, 2, 'one retry');
  await assert.rejects(up.request('GET', '/v1/events'));
  assert.equal(breaker.state, 'open');
  await assert.rejects(up.request('GET', '/v1/events'), (e) => e.status === 503 && e.retryAfter === 5);
  assert.equal(calls, 4, 'no calls while open');
});

test('upstream does not count 4xx or shed-503 against the breaker', async () => {
  const statuses = [409, 404, 503, 503, 503];
  const fetchImpl = async () => new Response(JSON.stringify({ error: 'x' }), { status: statuses.shift() });
  const breaker = new CircuitBreaker({ name: 'booking', threshold: 1 });
  const up = new Upstream({ baseUrl: 'http://x', timeoutMs: 100, breaker, fetchImpl });
  for (let i = 0; i < 5; i++) await up.request('POST', '/v1/reservations', { body: {} });
  assert.equal(breaker.state, 'closed');
});

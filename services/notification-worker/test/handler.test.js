import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { after, before, test } from 'node:test';

import pg from 'pg';

import { createHandler, PermanentError, processWithRetry } from '../src/handler.js';

const raw = (obj) => ({ key: Buffer.from('k'), value: Buffer.from(typeof obj === 'string' ? obj : JSON.stringify(obj)), offset: '1', headers: {} });
const quiet = { warn() {} };

test('poison messages go straight to the DLQ without retries', async () => {
  let calls = 0;
  const dlq = [];
  const out = await processWithRetry(raw('{not json'), {
    handle: async () => calls++,
    sendToDlq: async (m, e) => dlq.push(e.message),
    log: quiet,
  });
  assert.equal(out, 'dead-lettered');
  assert.equal(calls, 0);
  assert.deepEqual(dlq, ['invalid JSON']);
});

test('transient failures are retried, then succeed', async () => {
  let calls = 0;
  const out = await processWithRetry(raw({ event_id: 'e', type: 't' }), {
    handle: async () => {
      if (++calls < 3) throw new Error('db blip');
      return 'processed';
    },
    sendToDlq: async () => assert.fail('should not dead-letter'),
    baseDelayMs: 1,
    log: quiet,
  });
  assert.equal(out, 'processed');
  assert.equal(calls, 3);
});

test('a message that keeps failing is dead-lettered after N attempts', async () => {
  let calls = 0;
  const dlq = [];
  const out = await processWithRetry(raw({ event_id: 'e', type: 't' }), {
    handle: async () => {
      calls++;
      throw new Error('downstream down');
    },
    sendToDlq: async (m) => dlq.push(m),
    attempts: 3,
    baseDelayMs: 1,
    log: quiet,
  });
  assert.equal(out, 'dead-lettered');
  assert.equal(calls, 3);
  assert.equal(dlq.length, 1);
});

test('if the DLQ is unavailable the error propagates (Kafka will redeliver)', async () => {
  await assert.rejects(
    processWithRetry(raw('x'), {
      handle: async () => {},
      sendToDlq: async () => {
        throw new Error('kafka down');
      },
      log: quiet,
    }),
    /kafka down/,
  );
});

// --- against real Postgres (tables created by booking-service migrations) ---
const url = process.env.TEST_DATABASE_URL;
let pool;
before(() => {
  if (url) pool = new pg.Pool({ connectionString: url, max: 20 });
});
after(() => pool?.end());

test('duplicate deliveries produce exactly one notification', { skip: !url }, async () => {
  const handle = createHandler({ pool });
  const msg = {
    event_id: randomUUID(),
    type: 'reservation.confirmed',
    aggregate_id: 'r1',
    data: { id: 'r1', user_id: 'alice', status: 'CONFIRMED' },
  };
  // The same event delivered 20 times concurrently (rebalance storm).
  const outcomes = await Promise.all(Array.from({ length: 20 }, () => handle(msg)));
  assert.equal(outcomes.filter((o) => o === 'processed').length, 1);
  assert.equal(outcomes.filter((o) => o === 'duplicate').length, 19);
  const { rows } = await pool.query('SELECT count(*)::int AS n FROM notifications WHERE event_id = $1', [msg.event_id]);
  assert.equal(rows[0].n, 1);
});

test('events without a user are permanent errors', { skip: !url }, async () => {
  const handle = createHandler({ pool });
  await assert.rejects(handle({ event_id: randomUUID(), type: 'reservation.held', data: {} }), PermanentError);
});

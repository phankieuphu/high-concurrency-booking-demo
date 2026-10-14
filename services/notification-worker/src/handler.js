// Business handler + delivery semantics for booking events.
//
// Kafka + the outbox give us AT-LEAST-ONCE delivery: the same event can
// arrive twice (relay crash, consumer rebalance, redelivery after a timeout).
// We turn that into EXACTLY-ONCE EFFECT by recording the event id and the
// side effect in the same Postgres transaction.

export const CONSUMER_NAME = 'notification-worker';

const TEMPLATES = {
  'reservation.held': { template: 'hold_created', channel: 'push' },
  'reservation.confirmed': { template: 'ticket_confirmed', channel: 'email' },
  'reservation.expired': { template: 'hold_expired', channel: 'push' },
  'reservation.cancelled': { template: 'hold_cancelled', channel: 'push' },
  'reservation.payment_failed': { template: 'payment_failed', channel: 'email' },
};

export class PermanentError extends Error {}

export function parseEnvelope(buf) {
  let msg;
  try {
    msg = JSON.parse(buf.toString());
  } catch {
    throw new PermanentError('invalid JSON');
  }
  if (!msg?.event_id || !msg?.type) throw new PermanentError('missing event_id/type');
  return msg;
}

export function createHandler({ pool }) {
  return async function handle(msg) {
    const spec = TEMPLATES[msg.type];
    if (!spec) return 'ignored';
    const data = msg.data?.reservation ?? msg.data ?? {};
    if (!data.user_id) throw new PermanentError(`event ${msg.event_id} has no user_id`);

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const first = await client.query(
        `INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2)
         ON CONFLICT DO NOTHING RETURNING event_id`,
        [CONSUMER_NAME, msg.event_id],
      );
      if (first.rowCount === 0) {
        await client.query('ROLLBACK');
        return 'duplicate';
      }
      // The "side effect": an outbound notification row. A sender process
      // delivers these with the provider's own idempotency key = event_id.
      await client.query(
        `INSERT INTO notifications (event_id, user_id, channel, template, payload)
         VALUES ($1, $2, $3, $4, $5)`,
        [msg.event_id, data.user_id, spec.channel, spec.template, JSON.stringify(msg.data)],
      );
      await client.query('COMMIT');
      return 'processed';
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Retry transient failures in-process with exponential backoff + jitter; send
 * poison / permanently failing messages to the DLQ so one bad message never
 * blocks its partition. Returns the outcome; throws only if even the DLQ
 * publish fails (so Kafka redelivers instead of losing the message).
 */
export async function processWithRetry(raw, { handle, sendToDlq, attempts = 5, baseDelayMs = 100, log = console }) {
  let lastErr;
  let msg;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      msg ??= parseEnvelope(raw.value);
      return await handle(msg);
    } catch (err) {
      lastErr = err;
      if (err instanceof PermanentError) break;
      if (attempt < attempts) {
        const delay = Math.min(baseDelayMs * 2 ** (attempt - 1), 5000);
        await sleep(delay / 2 + Math.random() * (delay / 2));
      }
    }
  }
  log.warn?.({ err: lastErr?.message, offset: raw.offset }, 'sending message to DLQ');
  await sendToDlq(raw, lastErr);
  return 'dead-lettered';
}

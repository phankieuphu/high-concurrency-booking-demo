import { Kafka, logLevel } from 'kafkajs';
import pg from 'pg';

import { CONSUMER_NAME, createHandler, processWithRetry } from './handler.js';

const env = process.env;
const topic = env.KAFKA_TOPIC || 'booking.events';
const dlqTopic = env.KAFKA_DLQ_TOPIC || `${topic}.dlq`;

const log = {
  info: (o, m) => console.log(JSON.stringify({ level: 'info', msg: m, ...o })),
  warn: (o, m) => console.log(JSON.stringify({ level: 'warn', msg: m, ...o })),
  error: (o, m) => console.log(JSON.stringify({ level: 'error', msg: m, ...o })),
};

const pool = new pg.Pool({
  connectionString: env.DATABASE_URL || 'postgres://booking:booking@localhost:5432/booking',
  max: Number(env.DB_POOL_SIZE || 10),
  connectionTimeoutMillis: 3000,
  idleTimeoutMillis: 30_000,
  statement_timeout: 5000,
});
pool.on('error', (err) => log.warn({ err: err.message }, 'idle pg client error'));

const kafka = new Kafka({
  clientId: env.HOSTNAME || CONSUMER_NAME,
  brokers: (env.KAFKA_BROKERS || 'localhost:9092').split(','),
  logLevel: logLevel.WARN,
  retry: { initialRetryTime: 300, retries: 10 },
});
const consumer = kafka.consumer({
  groupId: env.KAFKA_GROUP || CONSUMER_NAME,
  sessionTimeout: 30_000,
  heartbeatInterval: 3000,
});
const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });

const handle = createHandler({ pool });
const sendToDlq = async (raw, err) => {
  await producer.send({
    topic: dlqTopic,
    acks: -1,
    messages: [
      {
        key: raw.key,
        value: raw.value,
        headers: {
          ...raw.headers,
          'x-error': String(err?.message || 'unknown').slice(0, 500),
          'x-original-topic': topic,
          'x-original-offset': String(raw.offset),
          'x-failed-at': new Date().toISOString(),
        },
      },
    ],
  });
};

const stats = { processed: 0, duplicate: 0, ignored: 0, 'dead-lettered': 0 };

async function main() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  // Partition key = reservation id, so events for one reservation are handled
  // in order; partitions are processed in parallel across consumers.
  await consumer.run({
    partitionsConsumedConcurrently: Number(env.PARTITION_CONCURRENCY || 4),
    eachMessage: async ({ message, partition }) => {
      const outcome = await processWithRetry(message, { handle, sendToDlq, log });
      stats[outcome] = (stats[outcome] || 0) + 1;
      if (outcome === 'dead-lettered') log.warn({ partition, offset: message.offset }, 'dead-lettered');
    },
  });
  log.info({ topic, dlqTopic }, 'notification-worker started');
  setInterval(() => log.info({ stats }, 'stats'), 30_000).unref();
}

let stopping = false;
for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, async () => {
    if (stopping) return;
    stopping = true;
    log.info({ sig }, 'shutting down');
    // Disconnect commits processed offsets and leaves the group cleanly so
    // partitions are reassigned immediately rather than after session timeout.
    await consumer.disconnect().catch(() => {});
    await producer.disconnect().catch(() => {});
    await pool.end().catch(() => {});
    process.exit(0);
  });
}

main().catch((err) => {
  log.error({ err: err.message }, 'fatal');
  process.exit(1);
});

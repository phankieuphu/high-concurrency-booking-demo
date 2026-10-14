import Redis from 'ioredis';

import { loadConfig } from './config.js';
import { CircuitBreaker } from './lib/circuitBreaker.js';
import { RateLimiter } from './lib/rateLimiter.js';
import { Upstream } from './lib/upstream.js';
import { WaitingRoom } from './lib/waitingRoom.js';
import { buildApp } from './server.js';

const config = loadConfig();

const redis = new Redis(config.redisUrl, {
  // Fail fast: never let a slow Redis stall request handling.
  commandTimeout: 200,
  maxRetriesPerRequest: 1,
  enableOfflineQueue: false,
  retryStrategy: (times) => Math.min(times * 200, 2000),
});
redis.on('error', () => {}); // surfaced through breakers/metrics instead of crashing

let app;
const onBreakerChange = (name, from, to) => app?.log.warn({ breaker: name, from, to }, 'circuit breaker state change');
const bookingBreaker = new CircuitBreaker({ name: 'booking', threshold: 10, cooldownMs: 5000, onChange: onBreakerChange });
const redisBreaker = new CircuitBreaker({ name: 'redis', threshold: 5, cooldownMs: 3000, onChange: onBreakerChange });

const upstream = new Upstream({
  baseUrl: config.bookingUrl,
  timeoutMs: config.upstreamTimeoutMs,
  breaker: bookingBreaker,
  internalToken: config.internalToken,
});
const rateLimiter = new RateLimiter({ redis, breaker: redisBreaker, ...config.rateLimit });
const waitingRoom = new WaitingRoom({
  redis,
  queueSecret: config.queueSecret,
  admissionTtlSec: config.admissionTtlSec,
  defaultRate: config.admitRatePerSec,
  instanceId: config.instanceId,
  upstreamHealth: () => bookingBreaker.state,
});

app = buildApp({ config, upstream, rateLimiter, waitingRoom, breakers: [bookingBreaker, redisBreaker] });
waitingRoom.log = app.log;
waitingRoom.startAdmitter();

await app.listen({ port: config.port, host: '0.0.0.0' });

// Graceful shutdown: fail readiness first so the load balancer drains us,
// then stop accepting connections and let in-flight requests complete.
let stopping = false;
for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, async () => {
    if (stopping) return;
    stopping = true;
    app.log.info({ sig }, 'draining');
    app.drain();
    waitingRoom.stopAdmitter();
    await new Promise((r) => setTimeout(r, config.drainDelayMs));
    await app.close();
    redis.disconnect();
    process.exit(0);
  });
}

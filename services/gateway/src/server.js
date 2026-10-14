import Fastify from 'fastify';
import client from 'prom-client';

import { issueDevUserToken, verifyUserToken } from './lib/tokens.js';
import { UpstreamError } from './lib/upstream.js';

/**
 * Builds the gateway app. Dependencies are injected so tests can run without
 * Redis or the booking service.
 */
export function buildApp({ config, upstream, rateLimiter, waitingRoom, breakers = [], logger = true }) {
  const app = Fastify({
    logger: logger && { level: process.env.LOG_LEVEL || 'info' },
    requestIdHeader: 'x-request-id',
    bodyLimit: 16 * 1024,
    connectionTimeout: 10_000,
    keepAliveTimeout: 65_000, // > typical cloud LB idle timeout (60s)
  });

  // ---- metrics -------------------------------------------------------------
  const registry = new client.Registry();
  client.collectDefaultMetrics({ register: registry });
  const httpDuration = new client.Histogram({
    name: 'gateway_http_request_duration_seconds',
    help: 'Gateway request latency',
    labelNames: ['route', 'code'],
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
    registers: [registry],
  });
  const shed = new client.Counter({ name: 'gateway_shed_total', help: 'Requests shed (in-flight limit)', registers: [registry] });
  const limited = new client.Counter({ name: 'gateway_rate_limited_total', help: 'Requests rate limited', registers: [registry] });
  new client.Gauge({
    name: 'gateway_breaker_open',
    help: '1 when a circuit breaker is not closed',
    labelNames: ['name'],
    registers: [registry],
    collect() {
      for (const b of breakers) this.set({ name: b.name }, b.state === 'closed' ? 0 : 1);
    },
  });

  // ---- load shedding & draining -------------------------------------------
  let inFlight = 0;
  let draining = false;
  app.decorate('drain', () => {
    draining = true;
  });

  app.addHook('onRequest', async (req, reply) => {
    req.startedAt = process.hrtime.bigint();
    if (req.url.startsWith('/livez') || req.url.startsWith('/readyz') || req.url.startsWith('/metrics')) return;
    if (inFlight >= config.maxInFlight) {
      shed.inc();
      return reply.code(503).header('retry-after', '1').send({ error: 'overloaded, retry shortly' });
    }
    inFlight += 1;
    req.counted = true;
  });
  const done = (req) => {
    if (req.counted) {
      req.counted = false;
      inFlight -= 1;
    }
  };
  app.addHook('onResponse', async (req, reply) => {
    done(req);
    const secs = Number(process.hrtime.bigint() - req.startedAt) / 1e9;
    httpDuration.labels(req.routeOptions?.url || 'unmatched', String(reply.statusCode)).observe(secs);
  });
  app.addHook('onRequestAbort', async (req) => done(req));

  app.setErrorHandler((err, req, reply) => {
    if (err instanceof UpstreamError) {
      return reply.code(err.status).header('retry-after', String(err.retryAfter)).send({ error: err.message });
    }
    if (err.validation || err.statusCode === 400) return reply.code(400).send({ error: err.message });
    req.log.error(err);
    return reply.code(err.statusCode && err.statusCode < 500 ? err.statusCode : 500).send({ error: 'internal error' });
  });

  // ---- helpers --------------------------------------------------------------
  const authenticate = async (req, reply) => {
    const user = verifyUserToken(req.headers.authorization, config.jwtSecret);
    if (!user) return reply.code(401).send({ error: 'unauthorized' });
    req.user = user;
  };

  const rateLimit = (cost = 1) => async (req, reply) => {
    const key = req.user ? `u:${req.user.userId}` : `ip:${req.ip}`;
    const r = await rateLimiter.take(key, cost);
    reply.header('x-ratelimit-remaining', r.remaining);
    if (!r.allowed) {
      limited.inc();
      return reply.code(429).header('retry-after', '1').send({ error: 'too many requests' });
    }
  };

  const forward = (reply, res) => {
    for (const [k, v] of Object.entries(res.headers)) if (v) reply.header(k, v);
    return reply.code(res.status).send(res.body);
  };

  const userHeaders = (req, extra = {}) => ({ 'x-user-id': req.user.userId, 'x-request-id': req.id, ...extra });

  // Micro-cache (1s) for catalog reads. With a CDN in front this is the
  // second layer; even without one, a stampede on a hot event page becomes
  // one upstream call per second per gateway instance.
  const cache = new Map();
  const cachedGet = async (path) => {
    const hit = cache.get(path);
    const now = Date.now();
    if (hit && hit.expires > now) return hit.value;
    if (hit?.pending) return hit.pending;
    const pending = upstream
      .request('GET', path)
      .then((value) => {
        if (value.status === 200) cache.set(path, { value, expires: Date.now() + 1000 });
        else cache.delete(path);
        return value;
      })
      .catch((err) => {
        // Serve stale catalog data if the booking service is struggling.
        if (hit?.value) {
          cache.set(path, { value: hit.value, expires: Date.now() + 1000 });
          return hit.value;
        }
        cache.delete(path);
        throw err;
      });
    cache.set(path, { ...hit, pending });
    if (cache.size > 10_000) cache.delete(cache.keys().next().value);
    return pending;
  };

  // ---- ops ------------------------------------------------------------------
  app.get('/livez', async () => ({ status: 'ok' }));
  app.get('/readyz', async (req, reply) => {
    if (draining) return reply.code(503).send({ status: 'draining' });
    return { status: 'ok', in_flight: inFlight, breakers: Object.fromEntries(breakers.map((b) => [b.name, b.state])) };
  });
  app.get('/metrics', async (req, reply) => reply.type(registry.contentType).send(await registry.metrics()));

  if (config.devAuth) {
    app.post('/auth/dev-token', async (req, reply) => {
      const userId = String(req.body?.user_id || '');
      if (!/^[\w-]{1,64}$/.test(userId)) return reply.code(400).send({ error: 'user_id required' });
      return { token: issueDevUserToken(userId, config.jwtSecret) };
    });
  }

  // ---- catalog (public, cacheable) -------------------------------------------
  app.get('/api/v1/events', async (req, reply) => forward(reply, await cachedGet('/v1/events')));
  app.get('/api/v1/events/:id', async (req, reply) => {
    if (!/^\d+$/.test(req.params.id)) return reply.code(404).send({ error: 'event not found' });
    return forward(reply, await cachedGet(`/v1/events/${req.params.id}`));
  });

  // ---- waiting room ----------------------------------------------------------
  app.post('/api/v1/queue/:eventId/join', { preHandler: [authenticate, rateLimit(1)] }, async (req, reply) => {
    const { eventId } = req.params;
    if (!/^\d+$/.test(eventId)) return reply.code(404).send({ error: 'event not found' });
    try {
      return await waitingRoom.join(req.user.userId, eventId);
    } catch (err) {
      req.log.warn({ err: err.message }, 'waiting room join failed');
      return reply.code(503).header('retry-after', '2').send({ error: 'queue temporarily unavailable, retry' });
    }
  });

  // Hot path: no Redis, no upstream. Deliberately not rate limited per call
  // beyond the in-flight guard; clients are told how often to poll.
  app.get('/api/v1/queue/:eventId/status', { preHandler: [authenticate] }, async (req, reply) => {
    const ticket = req.headers['x-queue-ticket'] || req.query.ticket;
    const s = await waitingRoom.status(req.user.userId, req.params.eventId, ticket);
    if (!s) return reply.code(400).send({ error: 'invalid or expired queue ticket; join again' });
    return s;
  });

  // ---- booking ---------------------------------------------------------------
  app.post('/api/v1/reservations', { preHandler: [authenticate, rateLimit(1)] }, async (req, reply) => {
    const body = req.body || {};
    const idem = req.headers['idempotency-key'];
    if (!idem || String(idem).length > 128) {
      return reply.code(400).send({ error: 'Idempotency-Key header (<=128 chars) is required' });
    }
    if (!Number.isInteger(body.event_id) || !Number.isInteger(body.tier_id) || !Number.isInteger(body.quantity)) {
      return reply.code(400).send({ error: 'event_id, tier_id and quantity must be integers' });
    }

    // Enforce the waiting room for hot events: without a valid admission
    // token the request never reaches the booking service.
    const ev = await cachedGet(`/v1/events/${body.event_id}`);
    if (ev.status === 404) return reply.code(404).send({ error: 'event not found' });
    if (ev.status !== 200) return forward(reply, ev);
    if (ev.body.waiting_room) {
      const token = req.headers['x-admission-token'];
      if (!waitingRoom.isAdmitted(req.user.userId, body.event_id, token)) {
        return reply.code(403).send({ error: 'waiting room admission required', join: `/api/v1/queue/${body.event_id}/join` });
      }
    }

    const res = await upstream.request('POST', '/v1/reservations', {
      headers: userHeaders(req, { 'idempotency-key': String(idem) }),
      body: { event_id: body.event_id, tier_id: body.tier_id, quantity: body.quantity },
    });
    return forward(reply, res);
  });

  const idParam = /^[0-9a-fA-F-]{36}$/;
  app.get('/api/v1/reservations/:id', { preHandler: [authenticate] }, async (req, reply) => {
    if (!idParam.test(req.params.id)) return reply.code(404).send({ error: 'not found' });
    return forward(reply, await upstream.request('GET', `/v1/reservations/${req.params.id}`, { headers: userHeaders(req) }));
  });

  app.post('/api/v1/reservations/:id/confirm', { preHandler: [authenticate, rateLimit(2)] }, async (req, reply) => {
    if (!idParam.test(req.params.id)) return reply.code(404).send({ error: 'not found' });
    const res = await upstream.request('POST', `/v1/reservations/${req.params.id}/confirm`, {
      headers: userHeaders(req),
      body: { payment_token: req.body?.payment_token },
    });
    return forward(reply, res);
  });

  app.post('/api/v1/reservations/:id/cancel', { preHandler: [authenticate, rateLimit(1)] }, async (req, reply) => {
    if (!idParam.test(req.params.id)) return reply.code(404).send({ error: 'not found' });
    return forward(reply, await upstream.request('POST', `/v1/reservations/${req.params.id}/cancel`, { headers: userHeaders(req) }));
  });

  return app;
}

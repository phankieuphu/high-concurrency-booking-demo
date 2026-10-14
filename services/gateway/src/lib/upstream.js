import { CircuitOpenError } from './circuitBreaker.js';

export class UpstreamError extends Error {
  constructor(message, { status = 503, retryAfter = 1 } = {}) {
    super(message);
    this.status = status;
    this.retryAfter = retryAfter;
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * HTTP client for booking-service: per-call timeout, circuit breaker and a
 * single retry on connection-level failures. Retrying POSTs is safe because
 * every write carries an idempotency key (reserve) or is keyed by the
 * reservation id (confirm/cancel).
 */
export class Upstream {
  constructor({ baseUrl, timeoutMs, breaker, internalToken, fetchImpl = fetch }) {
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.timeoutMs = timeoutMs;
    this.breaker = breaker;
    this.internalToken = internalToken;
    this.fetch = fetchImpl;
  }

  async request(method, path, { headers = {}, body, retries = 1 } = {}) {
    for (let attempt = 0; ; attempt++) {
      try {
        return await this.breaker.exec(
          () => this.#once(method, path, headers, body),
          // 5xx and network errors count against the service; 4xx/409/503-shed are healthy answers.
          (err, res) => (err ? true : res.status >= 500 && res.status !== 503),
        );
      } catch (err) {
        if (err instanceof CircuitOpenError) {
          throw new UpstreamError('booking service temporarily unavailable', { status: 503, retryAfter: 5 });
        }
        const timedOut = err.name === 'TimeoutError' || err.name === 'AbortError';
        if (attempt < retries && !timedOut) {
          await sleep(25 + Math.random() * 75);
          continue;
        }
        throw new UpstreamError(timedOut ? 'booking service timeout' : 'booking service unreachable', {
          status: timedOut ? 504 : 503,
        });
      }
    }
  }

  async #once(method, path, headers, body) {
    const res = await this.fetch(this.baseUrl + path, {
      method,
      headers: {
        ...(body ? { 'content-type': 'application/json' } : {}),
        ...(this.internalToken ? { 'x-internal-token': this.internalToken } : {}),
        ...headers,
      },
      body: body ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    const text = await res.text();
    let json;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = { error: text };
    }
    return {
      status: res.status,
      body: json,
      headers: {
        'retry-after': res.headers.get('retry-after'),
        'cache-control': res.headers.get('cache-control'),
        'idempotent-replayed': res.headers.get('idempotent-replayed'),
      },
    };
  }
}

// Distributed token bucket in Redis (one atomic Lua call per request), with a
// per-instance in-memory fallback when Redis is unavailable: we keep limiting
// (fail-"soft") instead of either rejecting everyone or limiting no one.

const TOKEN_BUCKET_LUA = `
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local cost     = tonumber(ARGV[4])
local b = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(b[1]) or capacity
local ts     = tonumber(b[2]) or now
tokens = math.min(capacity, tokens + math.max(0, now - ts) * rate / 1000)
local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end
redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, math.ceil(capacity / rate * 1000) + 1000)
return { allowed, math.floor(tokens) }
`;

export class LocalTokenBucket {
  constructor({ capacity, refillPerSec, maxKeys = 100_000, now = Date.now }) {
    this.capacity = capacity;
    this.rate = refillPerSec;
    this.maxKeys = maxKeys;
    this.now = now;
    this.buckets = new Map();
  }

  take(key, cost = 1) {
    const now = this.now();
    let b = this.buckets.get(key);
    if (!b) {
      if (this.buckets.size >= this.maxKeys) {
        // Bounded memory: evict the oldest entry (Map keeps insertion order).
        this.buckets.delete(this.buckets.keys().next().value);
      }
      b = { tokens: this.capacity, ts: now };
      this.buckets.set(key, b);
    }
    b.tokens = Math.min(this.capacity, b.tokens + ((now - b.ts) * this.rate) / 1000);
    b.ts = now;
    if (b.tokens >= cost) {
      b.tokens -= cost;
      return { allowed: true, remaining: Math.floor(b.tokens) };
    }
    return { allowed: false, remaining: 0 };
  }
}

export class RateLimiter {
  constructor({ redis, breaker, capacity, refillPerSec }) {
    this.redis = redis;
    this.breaker = breaker;
    this.capacity = capacity;
    this.rate = refillPerSec;
    this.local = new LocalTokenBucket({ capacity, refillPerSec });
    if (redis) redis.defineCommand('tokenBucket', { numberOfKeys: 1, lua: TOKEN_BUCKET_LUA });
  }

  async take(key, cost = 1) {
    if (!this.redis) return { ...this.local.take(key, cost), source: 'local' };
    try {
      const [allowed, remaining] = await this.breaker.exec(() =>
        this.redis.tokenBucket(`rl:{${key}}`, this.capacity, this.rate, Date.now(), cost),
      );
      return { allowed: allowed === 1, remaining, source: 'redis' };
    } catch {
      return { ...this.local.take(key, cost), source: 'local' };
    }
  }
}

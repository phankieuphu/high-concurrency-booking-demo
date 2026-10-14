// Virtual waiting room.
//
// At sale open, 1M people press "buy" in the same second, but the booking tier
// can usefully process a few thousand reservations per second. Instead of
// letting the excess hammer the system (timeouts, retries, collapse), we hand
// every user a FIFO position and admit them at a controlled rate.
//
// Scaling properties:
//   * join   : 1 Redis Lua call (idempotent per user) -> signed ticket {seq}
//   * status : ZERO Redis calls. The ticket carries seq; the admitted
//              watermark is cached per gateway instance and refreshed once
//              per second. 1M users polling every 5s = 200k rps, all served
//              from gateway memory.
//   * admit  : one leader (Redis lock) advances the watermark every second.
//              Rate drops to zero automatically when the booking service is
//              unhealthy (backpressure from the circuit breaker).

import { signAdmission, signQueueTicket, verifyQueueToken } from './tokens.js';

const JOIN_LUA = `
local s = redis.call('HGET', KEYS[2], ARGV[1])
if s then return tonumber(s) end
s = redis.call('INCR', KEYS[1])
redis.call('HSET', KEYS[2], ARGV[1], s)
redis.call('EXPIRE', KEYS[1], 86400)
redis.call('EXPIRE', KEYS[2], 86400)
return s
`;

const ADVANCE_LUA = `
local seq = tonumber(redis.call('GET', KEYS[1]) or '0')
local adm = tonumber(redis.call('GET', KEYS[2]) or '0')
local nxt = math.min(adm + tonumber(ARGV[1]), seq)
if nxt > adm then redis.call('SET', KEYS[2], nxt, 'EX', 86400) end
return nxt
`;

// Renew the leader lock only if we still own it.
const RENEW_LUA = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`;

const keys = (eventId) => ({
  seq: `wr:{${eventId}}:seq`,
  users: `wr:{${eventId}}:users`,
  admitted: `wr:{${eventId}}:admitted`,
  rate: `wr:{${eventId}}:rate`,
});

export class WaitingRoom {
  constructor({ redis, queueSecret, admissionTtlSec, defaultRate, instanceId, upstreamHealth, log, now = Date.now }) {
    this.redis = redis;
    this.secret = queueSecret;
    this.admissionTtlSec = admissionTtlSec;
    this.defaultRate = defaultRate;
    this.instanceId = instanceId;
    this.upstreamHealth = upstreamHealth; // () => 'closed' | 'half-open' | 'open'
    this.log = log;
    this.now = now;
    this.watermarks = new Map(); // eventId -> { value, fetchedAt, pending }
    this.leaderTimer = null;
    this.isLeader = false;
    redis.defineCommand('wrJoin', { numberOfKeys: 2, lua: JOIN_LUA });
    redis.defineCommand('wrAdvance', { numberOfKeys: 2, lua: ADVANCE_LUA });
    redis.defineCommand('wrRenew', { numberOfKeys: 1, lua: RENEW_LUA });
  }

  async join(userId, eventId) {
    const k = keys(eventId);
    const seq = Number(await this.redis.wrJoin(k.seq, k.users, userId));
    await this.redis.sadd('wr:active', String(eventId));
    const ticket = signQueueTicket({ userId, eventId, seq }, this.secret);
    return { ticket, ...(await this.status(userId, eventId, ticket)) };
  }

  async status(userId, eventId, ticket) {
    const claims = verifyQueueToken(ticket, this.secret, { typ: 'queue', userId, eventId });
    if (!claims) return null;
    const admitted = await this.watermark(eventId);
    if (claims.seq <= admitted) {
      return {
        state: 'admitted',
        position: 0,
        admission_token: signAdmission({ userId, eventId }, this.secret, this.admissionTtlSec),
        admission_ttl_sec: this.admissionTtlSec,
      };
    }
    const position = claims.seq - admitted;
    return {
      state: 'waiting',
      position,
      estimated_wait_sec: Math.ceil(position / Math.max(1, this.defaultRate)),
      poll_after_sec: position > 10_000 ? 10 : 3, // far-back users poll less
    };
  }

  isAdmitted(userId, eventId, admissionToken) {
    return Boolean(verifyQueueToken(admissionToken, this.secret, { typ: 'admission', userId, eventId }));
  }

  /** Watermark cached for 1s per instance; concurrent misses share one fetch. */
  async watermark(eventId) {
    const now = this.now();
    let w = this.watermarks.get(eventId);
    if (w && now - w.fetchedAt < 1000) return w.value;
    if (w?.pending) return w.pending;
    const pending = this.redis
      .get(keys(eventId).admitted)
      .then((v) => {
        const value = Number(v || 0);
        this.watermarks.set(eventId, { value, fetchedAt: this.now(), pending: null });
        return value;
      })
      .catch((err) => {
        // Redis trouble: keep serving the last known value (users just wait a bit longer).
        this.watermarks.set(eventId, { value: w?.value ?? 0, fetchedAt: w?.fetchedAt ?? 0, pending: null });
        this.log?.warn({ err: err.message }, 'watermark refresh failed');
        return w?.value ?? 0;
      });
    this.watermarks.set(eventId, { value: w?.value ?? 0, fetchedAt: w?.fetchedAt ?? 0, pending });
    return pending;
  }

  /** Effective admission rate after backpressure. */
  effectiveRate(configured) {
    const health = this.upstreamHealth?.() ?? 'closed';
    if (health === 'open') return 0; // booking service down: stop letting people in
    if (health === 'half-open') return Math.max(1, Math.floor(configured / 4));
    return configured;
  }

  async tick() {
    const events = await this.redis.smembers('wr:active');
    for (const eventId of events) {
      const k = keys(eventId);
      const configured = Number((await this.redis.get(k.rate)) || this.defaultRate);
      const rate = this.effectiveRate(configured);
      if (rate > 0) await this.redis.wrAdvance(k.seq, k.admitted, rate);
    }
  }

  /** Leader election via SET NX PX; only the leader advances watermarks. */
  startAdmitter({ intervalMs = 1000, lockTtlMs = 3000 } = {}) {
    const lockKey = 'wr:admitter:leader';
    const loop = async () => {
      try {
        if (this.isLeader) {
          this.isLeader = (await this.redis.wrRenew(lockKey, this.instanceId, lockTtlMs)) === 1;
        } else {
          this.isLeader = (await this.redis.set(lockKey, this.instanceId, 'PX', lockTtlMs, 'NX')) === 'OK';
          if (this.isLeader) this.log?.info('became waiting-room admitter');
        }
        if (this.isLeader) await this.tick();
      } catch (err) {
        this.isLeader = false;
        this.log?.warn({ err: err.message }, 'admitter tick failed');
      }
    };
    this.leaderTimer = setInterval(loop, intervalMs);
    this.leaderTimer.unref();
    return loop();
  }

  stopAdmitter() {
    clearInterval(this.leaderTimer);
  }
}

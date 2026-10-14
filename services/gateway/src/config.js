const num = (v, d) => (v === undefined || v === '' || Number.isNaN(Number(v)) ? d : Number(v));

export function loadConfig(env = process.env) {
  return {
    port: num(env.PORT, 8080),
    instanceId: env.HOSTNAME || `gw-${process.pid}`,
    bookingUrl: env.BOOKING_URL || 'http://localhost:8081',
    internalToken: env.INTERNAL_TOKEN || '',
    upstreamTimeoutMs: num(env.UPSTREAM_TIMEOUT_MS, 6000),

    // Same HS256 secret/claims as the existing user-service (pkg/jwtutil).
    jwtSecret: env.JWT_SECRET || 'dev-secret-change-me',
    devAuth: env.DEV_AUTH === 'true',

    // Separate key for waiting-room tickets & admission tokens.
    queueSecret: env.QUEUE_SECRET || 'dev-queue-secret-change-me',
    admissionTtlSec: num(env.ADMISSION_TTL_SEC, 600),
    // Default users admitted per second per event, sized to what the
    // booking tier can absorb (see docs/01-capacity-planning.md).
    admitRatePerSec: num(env.ADMIT_RATE_PER_SEC, 500),

    redisUrl: env.REDIS_URL || 'redis://localhost:6379',

    maxInFlight: num(env.MAX_IN_FLIGHT, 5000),
    rateLimit: {
      capacity: num(env.RATE_LIMIT_BURST, 10),
      refillPerSec: num(env.RATE_LIMIT_PER_SEC, 5),
    },
    drainDelayMs: num(env.DRAIN_DELAY_MS, 5000),
  };
}

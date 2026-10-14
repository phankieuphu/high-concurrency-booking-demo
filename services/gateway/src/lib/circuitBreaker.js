export class CircuitOpenError extends Error {
  constructor(name) {
    super(`circuit ${name} is open`);
    this.name = 'CircuitOpenError';
  }
}

/**
 * Consecutive-failure circuit breaker (closed -> open -> half-open -> closed).
 * While open, calls fail instantly so a sick dependency cannot pile up
 * thousands of pending sockets in the gateway.
 */
export class CircuitBreaker {
  constructor({ name, threshold = 5, cooldownMs = 5000, now = Date.now, onChange } = {}) {
    this.name = name;
    this.threshold = threshold;
    this.cooldownMs = cooldownMs;
    this.now = now;
    this.onChange = onChange;
    this.state = 'closed';
    this.failures = 0;
    this.openedAt = 0;
    this.probing = false;
  }

  allow() {
    if (this.state === 'open') {
      if (this.now() - this.openedAt < this.cooldownMs) return false;
      this.#set('half-open');
      this.probing = true;
      return true;
    }
    if (this.state === 'half-open') {
      if (this.probing) return false;
      this.probing = true;
    }
    return true;
  }

  success() {
    this.failures = 0;
    this.probing = false;
    if (this.state !== 'closed') this.#set('closed');
  }

  failure() {
    this.probing = false;
    this.failures += 1;
    if (this.state === 'half-open' || this.failures >= this.threshold) {
      this.openedAt = this.now();
      if (this.state !== 'open') this.#set('open');
    }
  }

  /** Runs fn; isFailure(err|result) decides what counts against the dependency. */
  async exec(fn, isFailure = (err) => Boolean(err)) {
    if (!this.allow()) throw new CircuitOpenError(this.name);
    try {
      const result = await fn();
      if (isFailure(null, result)) this.failure();
      else this.success();
      return result;
    } catch (err) {
      if (isFailure(err)) this.failure();
      else this.success();
      throw err;
    }
  }

  #set(state) {
    const from = this.state;
    this.state = state;
    this.onChange?.(this.name, from, state);
  }
}

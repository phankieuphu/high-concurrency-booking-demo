import jwt from 'jsonwebtoken';

/** Verifies user JWTs issued by user-service (HS256, `user_id` claim). */
export function verifyUserToken(header, secret) {
  if (!header) return null;
  const [scheme, token] = header.split(' ');
  if (!/^bearer$/i.test(scheme) || !token) return null;
  try {
    const claims = jwt.verify(token, secret, { algorithms: ['HS256'] });
    const userId = claims.user_id || claims.sub;
    return userId ? { userId: String(userId), claims } : null;
  } catch {
    return null;
  }
}

export function issueDevUserToken(userId, secret) {
  return jwt.sign({ user_id: userId, username: userId, role_ids: [], permissions: [] }, secret, {
    algorithm: 'HS256',
    subject: userId,
    expiresIn: '12h',
  });
}

// Waiting-room tickets and admission tokens are signed, so checking them is
// pure CPU: no Redis round trip on the hottest endpoint (status polling).

export function signQueueTicket({ userId, eventId, seq }, secret) {
  return jwt.sign({ typ: 'queue', eid: String(eventId), seq }, secret, {
    algorithm: 'HS256',
    subject: userId,
    expiresIn: '6h',
  });
}

export function signAdmission({ userId, eventId }, secret, ttlSec) {
  return jwt.sign({ typ: 'admission', eid: String(eventId) }, secret, {
    algorithm: 'HS256',
    subject: userId,
    expiresIn: ttlSec,
  });
}

export function verifyQueueToken(token, secret, { typ, userId, eventId }) {
  if (!token) return null;
  try {
    const c = jwt.verify(token, secret, { algorithms: ['HS256'] });
    if (c.typ !== typ || c.sub !== userId || c.eid !== String(eventId)) return null;
    return c;
  } catch {
    return null;
  }
}

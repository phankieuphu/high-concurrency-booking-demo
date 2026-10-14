// k6 flash-sale scenario. For 1M-user rehearsals run it distributed
// (k6 Operator on Kubernetes, or Grafana Cloud k6) from several regions.
//
//   k6 run -e GATEWAY=https://staging.example.com -e EVENT=1 -e TIER=2 loadtest/flash-sale.k6.js
//
// Phases mirror sale day: users arrive before the sale, everyone joins the
// queue at T0, polls status, and admitted users reserve + pay.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import exec from 'k6/execution';

const GW = __ENV.GATEWAY || 'http://localhost:8080';
const EVENT = Number(__ENV.EVENT || 1);
const TIER = Number(__ENV.TIER || 1);

const confirmed = new Counter('tickets_confirmed');
const soldOut = new Counter('sold_out');
const queueWait = new Trend('queue_wait_ms', true);

export const options = {
  scenarios: {
    sale_open: {
      executor: 'ramping-arrival-rate',
      startRate: 100,
      timeUnit: '1s',
      preAllocatedVUs: 2000,
      maxVUs: 50000,
      stages: [
        { target: 5000, duration: '10s' },   // the T0 spike
        { target: 5000, duration: '50s' },
        { target: 200, duration: '2m' },     // stragglers
      ],
    },
  },
  thresholds: {
    'http_req_failed{name:join}': ['rate<0.01'],
    'http_req_duration{name:status}': ['p(99)<100'],
    'http_req_duration{name:reserve}': ['p(95)<500', 'p(99)<1500'],
  },
};

const json = { 'content-type': 'application/json' };

export default function () {
  const user = `k6-${exec.scenario.iterationInTest}`;
  const tok = http.post(`${GW}/auth/dev-token`, JSON.stringify({ user_id: user }), { headers: json }).json('token');
  const auth = { ...json, authorization: `Bearer ${tok}` };

  const join = http.post(`${GW}/api/v1/queue/${EVENT}/join`, null, { headers: auth, tags: { name: 'join' } });
  if (!check(join, { joined: (r) => r.status === 200 })) return;
  const ticket = join.json('ticket');
  let st = join.json();
  const t0 = Date.now();
  while (st.state !== 'admitted') {
    sleep(st.poll_after_sec || 3);
    const r = http.get(`${GW}/api/v1/queue/${EVENT}/status`, { headers: { ...auth, 'x-queue-ticket': ticket }, tags: { name: 'status' } });
    if (r.status !== 200) return;
    st = r.json();
  }
  queueWait.add(Date.now() - t0);

  const headers = { ...auth, 'idempotency-key': `${user}-1`, 'x-admission-token': st.admission_token };
  let res;
  for (let i = 0; i < 5; i++) {
    res = http.post(`${GW}/api/v1/reservations`, JSON.stringify({ event_id: EVENT, tier_id: TIER, quantity: 2 }), { headers, tags: { name: 'reserve' } });
    if (![429, 503, 504].includes(res.status)) break;
    sleep(Number(res.headers['Retry-After'] || 1) * (1 + Math.random()));
  }
  if (res.status === 409) return soldOut.add(1);
  if (!check(res, { held: (r) => r.status === 201 || r.status === 200 })) return;

  sleep(2 + Math.random() * 5); // user types card details
  const pay = http.post(`${GW}/api/v1/reservations/${res.json('id')}/confirm`, JSON.stringify({ payment_token: 'tok_visa' }), { headers: auth, tags: { name: 'confirm' } });
  if (pay.status === 200) confirmed.add(1);
}

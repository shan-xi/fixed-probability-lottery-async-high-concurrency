import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const BASE_USER_ID = Number(__ENV.BASE_USER_ID || '100000000');
export const options = {
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || '1000'),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: Number(__ENV.PRE_VUS || '200'),
      maxVUs: Number(__ENV.MAX_VUS || '2000'),
    },
  },
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<1000', 'p(99)<2000'] },
};

export default function () {
  // VU and iteration make IDs unique across the test run for the usual k6 execution model.
  const userId = BASE_USER_ID + (__VU * 100000000) + __ITER;
  const response = http.post(`${BASE_URL}/api/lottery/draws`, JSON.stringify({ userId }), {
    headers: { 'Content-Type': 'application/json' },
  });
  check(response, {
    'status is created or idempotent replay': (r) => r.status === 201 || r.status === 200,
    'response has prize type': (r) => {
      try { return Boolean(JSON.parse(r.body).prizeType); } catch (_) { return false; }
    },
  });
}

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const base = __ENV.BASE_URL || 'http://localhost:8080';
const rate = Number(__ENV.RATE || 100);
const duration = __ENV.DURATION || '30s';
const vus = Number(__ENV.VUS || 100);
const failed = new Rate('lottery_submit_failures');
const accepted = new Counter('lottery_submissions_accepted');

export const options = { scenarios: { submits: { executor: 'constant-arrival-rate', rate, timeUnit: '1s', duration, preAllocatedVUs: vus, maxVUs: vus * 3 } } };

export default function () {
  const userId = (__VU * 100000000) + __ITER + 1;
  const res = http.post(`${base}/api/lottery/draws`, JSON.stringify({ userId }), { headers: { 'Content-Type': 'application/json' }, timeout: '10s' });
  const ok = check(res, { 'accepted or idempotent': r => r.status === 202 || r.status === 200 });
  failed.add(!ok);
  if (res.status === 202 || res.status === 200) accepted.add(1);
}

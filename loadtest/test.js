import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const base = __ENV.BASE_URL || 'http://localhost:8080';

const failed = new Rate('lottery_submit_failures');
const accepted = new Counter('lottery_submissions_accepted');

export const options = {
  scenarios: {
    lottery_concurrent_users: {
      executor: 'ramping-vus',

      stages: [
        // Warm up
        { duration: '10s', target: 100 },

        // 100 concurrent users
        { duration: '30s', target: 100 },

        // 500 concurrent users
        { duration: '30s', target: 500 },

        // 1,000 concurrent users
        { duration: '30s', target: 1000 },

        // 2,000 concurrent users
        { duration: '30s', target: 2000 },

        // Cool down
        { duration: '10s', target: 0 },
      ],

      gracefulRampDown: '30s',
    },
  },

  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500', 'p(99)<1000'],
    lottery_submit_failures: ['rate<0.01'],
  },
};

export default function () {
  // Unique user for every request
  const userId = (__VU * 100000000) + __ITER + 1;

  const payload = JSON.stringify({
    userId: userId,
  });

  const res = http.post(
    `${base}/api/lottery/draws`,
    payload,
    {
      headers: {
        'Content-Type': 'application/json',
      },
      timeout: '10s',
    }
  );

  const ok = check(res, {
    'accepted or idempotent': (r) =>
      r.status === 200 || r.status === 202,
  });

  failed.add(!ok);

  if (ok) {
    accepted.add(1);
  }
}
// Phase 1b — ClickHouse hot-tier load test: 50 concurrent users, 2 minutes.
// Simple aggregation query over the 2M-row bench.lineorder via the HTTP interface.
import http from 'k6/http';
import { check } from 'k6';
import { b64encode } from 'k6/encoding';

const USER = __ENV.CH_USER || 'dwh';
const PASS = __ENV.CH_PASS || '';
const HOST = __ENV.CH_HOST || 'localhost:18123';

const QUERY =
  'SELECT lo_year, count() AS orders, round(avg(lo_price),2) AS avg_price, ' +
  'sum(lo_quantity * lo_price) AS gross FROM bench.lineorder ' +
  'WHERE lo_quantity BETWEEN 5 AND 45 GROUP BY lo_year ORDER BY lo_year';

export const options = {
  scenarios: {
    hot_tier_agg: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 50 },   // ramp to 50 users
        { duration: '120s', target: 50 },  // hold 50 users
        { duration: '15s', target: 0 },    // ramp down
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.05'],
  },
};

const auth = `Basic ${b64encode(USER + ':' + PASS)}`;

export default function () {
  const res = http.post(
    `http://${HOST}/`,
    QUERY,
    {
      headers: {
        'Authorization': auth,
        'Content-Type': 'text/plain; charset=utf-8',
        'X-ClickHouse-Format': 'JSONCompact',
      },
    }
  );
  check(res, {
    'status 200': (r) => r.status === 200,
    'non-empty body': (r) => r.status === 200 && r.body.length > 10,
  });
}

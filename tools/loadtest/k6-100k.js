// AMeto ingest load test — OTLP JSON logs at a constant offered rate.
//
// Usage:
//   k6 run -e AMETO_API_KEY=<key> tools/loadtest/k6-100k.js
//   k6 run -e AMETO_API_KEY=<key> -e AMETO_URL=http://host:8555 -e RATE=50 -e BATCH=1000 -e DURATION=60s tools/loadtest/k6-100k.js
//
// Offered load = RATE requests/s x BATCH records = 100k logs/s by default.
// The whole batch JSON is built once at init and split on a timestamp
// placeholder; each iteration only does one native `parts.join(ts)` — so k6
// itself stays off the critical path even at 100k records/s.
//
// The server responds {"ingested":N,"dropped":M} per batch; both are summed
// into the ameto_ingested / ameto_dropped counters in the k6 summary.

import http from 'k6/http';
import { Counter } from 'k6/metrics';

const BATCH    = Number(__ENV.BATCH || 1000);       // records per request
const RATE     = Number(__ENV.RATE || 100);         // requests per second
const DURATION = __ENV.DURATION || '60s';
const BASE     = __ENV.AMETO_URL || 'http://localhost:8555';
const KEY      = __ENV.AMETO_API_KEY || '';         // Settings → API Keys (needs Logs permission)

if (!KEY) {
  throw new Error('AMETO_API_KEY is required: k6 run -e AMETO_API_KEY=<key> ...');
}

export const options = {
  scenarios: {
    ingest: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 64,
      maxVUs: 256,
    },
  },
};

const ingested = new Counter('ameto_ingested');
const dropped  = new Counter('ameto_dropped');

// ── Batch template (timestamp patched per iteration) ─────────────────────────
const SERVICES = ['Wallet.API', 'Processing.API', 'Etisalat.API', 'dealer.Gateway'];
const LEVELS   = [9, 9, 9, 5, 13]; // INFO x3, DEBUG, WARN
const ROUTES   = ['/api/pay', '/api/topup', '/api/status', '/dealer/api/wallet'];

function buildTemplate() {
  const perSvc = Math.max(1, Math.floor(BATCH / SERVICES.length));
  let rl = [];
  let n = 0;
  for (const svc of SERVICES) {
    let recs = [];
    for (let i = 0; i < perSvc; i++, n++) {
      recs.push(JSON.stringify({
        timeUnixNano: '@@TS@@',
        severityNumber: LEVELS[n % LEVELS.length],
        body: { stringValue: 'HTTP request handled' },
        attributes: [
          { key: 'orderId',          value: { intValue: String(1000000 + n) } },
          { key: 'customerId',       value: { stringValue: 'cust-' + (n % 500) } },
          { key: 'http.route',       value: { stringValue: ROUTES[n % ROUTES.length] } },
          { key: 'http.status_code', value: { intValue: String(n % 97 === 0 ? 500 : 200) } },
          { key: 'duration_ms',      value: { doubleValue: (n % 400) + 0.5 } },
          { key: 'cache_hit',        value: { boolValue: n % 3 === 0 } },
        ],
      }));
    }
    rl.push(
      '{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"' + svc + '"}}]},' +
      '"scopeLogs":[{"scope":{"name":"k6"},"logRecords":[' + recs.join(',') + ']}]}'
    );
  }
  return ('{"resourceLogs":[' + rl.join(',') + ']}').split('"@@TS@@"');
}

const PARTS  = buildTemplate();
const RECORDS_PER_REQ = PARTS.length - 1;
const PARAMS = {
  // Ingest auth is the Seq-compatible header, NOT X-Api-Key.
  headers: { 'Content-Type': 'application/json', 'X-Seq-ApiKey': KEY },
  timeout: '30s',
};

export default function () {
  const ts = '"' + (Date.now() * 1e6) + '"';
  const res = http.post(BASE + '/otlp/v1/logs', PARTS.join(ts), PARAMS);
  if (res.status === 200) {
    try {
      const j = JSON.parse(res.body);
      ingested.add(j.ingested);
      dropped.add(j.dropped);
    } catch (e) { /* ignore */ }
  } else {
    dropped.add(RECORDS_PER_REQ);
  }
}

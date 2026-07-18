// AMeto trace-ingest load test — OTLP JSON spans at a constant offered rate.
//
//   k6 run -e AMETO_API_KEY=<key> tools/loadtest/k6-traces.js
//   k6 run -e AMETO_API_KEY=<key> -e RATE=50 -e BATCH=1000 -e DURATION=60s tools/loadtest/k6-traces.js
//
// Offered load = RATE requests/s x BATCH spans = 50k spans/s by default.
// The batch is built once at init; per iteration three placeholders are patched:
// start/end timestamps and an 8-hex iteration prefix baked into every traceId/
// spanId/parentSpanId — so ids are unique per iteration while k6 only does a few
// native split/join passes per request.

import http from 'k6/http';
import { Counter } from 'k6/metrics';

const BATCH    = Number(__ENV.BATCH || 1000);
const RATE     = Number(__ENV.RATE || 50);
const DURATION = __ENV.DURATION || '60s';
const BASE     = __ENV.AMETO_URL || 'http://localhost:8555';
const KEY      = __ENV.AMETO_API_KEY || '';

if (!KEY) throw new Error('AMETO_API_KEY is required: k6 run -e AMETO_API_KEY=<key> ...');

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

const SERVICES = ['Wallet.API', 'Processing.API', 'Etisalat.API', 'dealer.Gateway'];
const ROUTES   = ['/api/pay', '/api/topup', '/api/status', '/dealer/api/wallet'];
const METHODS  = ['POST', 'GET', 'POST', 'PUT'];

function hex(n, width) {
  return n.toString(16).padStart(width, '0').slice(-width);
}

// Spans per service: a root SERVER span plus children referencing it, ~5 spans per trace.
function buildTemplate() {
  const perSvc = Math.max(1, Math.floor(BATCH / SERVICES.length));
  let rl = [];
  let n = 0;
  for (const svc of SERVICES) {
    let spans = [];
    for (let i = 0; i < perSvc; i++, n++) {
      const traceN  = Math.floor(n / 5);                  // 5 spans share a trace
      const isRoot  = n % 5 === 0;
      const traceId = '@@IT@@' + hex(traceN, 24);
      const spanId  = '@@IT@@' + hex(n, 8);
      const parent  = isRoot ? '' : '@@IT@@' + hex(Math.floor(n / 5) * 5, 8);
      const status  = n % 97 === 0 ? 500 : 200;

      spans.push(JSON.stringify({
        traceId: traceId,
        spanId: spanId,
        parentSpanId: parent,
        name: METHODS[n % 4] + ' ' + ROUTES[n % ROUTES.length],
        kind: isRoot ? 2 : 1,
        startTimeUnixNano: '@@TS@@',
        endTimeUnixNano: '@@TE@@',
        status: { code: status === 500 ? 2 : 1 },
        attributes: [
          { key: 'http.method',      value: { stringValue: METHODS[n % 4] } },
          { key: 'http.route',       value: { stringValue: ROUTES[n % ROUTES.length] } },
          { key: 'http.status_code', value: { intValue: String(status) } },
          { key: 'net.peer.name',    value: { stringValue: '10.220.0.' + (n % 250) } },
          { key: 'db.system',        value: { stringValue: 'mssql' } },
          { key: 'duration_ms',      value: { doubleValue: (n % 400) + 0.5 } },
        ],
      }));
    }
    rl.push(
      '{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"' + svc + '"}}]},' +
      '"scopeSpans":[{"scope":{"name":"k6"},"spans":[' + spans.join(',') + ']}]}'
    );
  }
  return ('{"resourceSpans":[' + rl.join(',') + ']}').split('@@IT@@');
}

const PARTS = buildTemplate();
const SPANS_PER_REQ = Math.max(1, Math.floor(BATCH / SERVICES.length)) * SERVICES.length;
const PARAMS = {
  headers: { 'Content-Type': 'application/json', 'X-Seq-ApiKey': KEY },
  timeout: '30s',
};

export default function () {
  const it = hex(__VU * 1000000 + __ITER, 8);            // unique ids per iteration
  const ts = String(Date.now() * 1e6);
  const te = String(Date.now() * 1e6 + 250 * 1e6);       // +250 ms duration
  const body = PARTS.join(it).split('@@TS@@').join(ts).split('@@TE@@').join(te);

  const res = http.post(BASE + '/otlp/v1/traces', body, PARAMS);
  if (res.status === 200) {
    try {
      const j = JSON.parse(res.body);
      ingested.add(j.ingested);
      dropped.add(j.dropped);
    } catch (e) { /* ignore */ }
  } else {
    dropped.add(SPANS_PER_REQ);
  }
}

// AMeto metric-ingest load test — OTLP JSON data points at a constant offered rate.
//
//   k6 run -e AMETO_API_KEY=<key> tools/loadtest/k6-metrics.js
//   k6 run -e AMETO_API_KEY=<key> -e RATE=50 -e SERIES=25 -e DURATION=60s tools/loadtest/k6-metrics.js
//
// Offered load = RATE requests/s x (40 metrics x SERIES label combos) data points —
// 50k points/s by default across 1000 distinct series: 15 counters + 15 gauges +
// 10 histograms (15 buckets each), service.name on the resource. The body is built
// once at init; only timeUnixNano is patched per iteration.

import http from 'k6/http';
import { Counter } from 'k6/metrics';

const RATE     = Number(__ENV.RATE || 50);
const SERIES   = Number(__ENV.SERIES || 25);   // label combos per metric
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

const ROUTES = ['/api/pay', '/api/topup', '/api/status', '/dealer/api/wallet', '/api/orders'];

function attrs(s) {
  return [
    { key: 'http.route', value: { stringValue: ROUTES[s % ROUTES.length] } },
    { key: 'instance',   value: { stringValue: 'node-' + (s % 5) } },
  ];
}

function numberPoints(base) {
  let pts = [];
  for (let s = 0; s < SERIES; s++) {
    pts.push(JSON.stringify({
      attributes: attrs(s),
      timeUnixNano: '@@TS@@',
      asDouble: base + s * 0.25,
    }));
  }
  return pts.join(',');
}

function histogramPoints() {
  const bounds = [1, 2, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 10000];
  let pts = [];
  for (let s = 0; s < SERIES; s++) {
    let counts = [];
    let total = 0;
    for (let b = 0; b <= bounds.length; b++) { const c = (s + b) % 40; counts.push(String(c)); total += c; }
    pts.push(JSON.stringify({
      attributes: attrs(s),
      timeUnixNano: '@@TS@@',
      count: String(total),
      sum: total * 12.5,
      bucketCounts: counts,
      explicitBounds: bounds,
    }));
  }
  return pts.join(',');
}

function buildTemplate() {
  let metrics = [];
  for (let m = 0; m < 15; m++)
    metrics.push('{"name":"ameto.loadtest.counter_' + m + '","unit":"1","sum":{"isMonotonic":true,"aggregationTemporality":2,"dataPoints":[' + numberPoints(m) + ']}}');
  for (let m = 0; m < 15; m++)
    metrics.push('{"name":"ameto.loadtest.gauge_' + m + '","unit":"By","gauge":{"dataPoints":[' + numberPoints(m * 10) + ']}}');
  for (let m = 0; m < 10; m++)
    metrics.push('{"name":"ameto.loadtest.duration_' + m + '","unit":"ms","histogram":{"aggregationTemporality":2,"dataPoints":[' + histogramPoints() + ']}}');

  const body =
    '{"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"LoadTest.API"}}]},' +
    '"scopeMetrics":[{"scope":{"name":"k6"},"metrics":[' + metrics.join(',') + ']}]}]}';
  return body.split('"@@TS@@"');
}

const PARTS = buildTemplate();
const POINTS_PER_REQ = 40 * SERIES;
const PARAMS = {
  headers: { 'Content-Type': 'application/json', 'X-Seq-ApiKey': KEY },
  timeout: '30s',
};

export default function () {
  const ts = '"' + (Date.now() * 1e6) + '"';
  const res = http.post(BASE + '/otlp/v1/metrics', PARTS.join(ts), PARAMS);
  if (res.status === 200) {
    try {
      const j = JSON.parse(res.body);
      ingested.add(j.ingested);
      dropped.add(j.dropped);
    } catch (e) { /* ignore */ }
  } else {
    dropped.add(POINTS_PER_REQ);
  }
}

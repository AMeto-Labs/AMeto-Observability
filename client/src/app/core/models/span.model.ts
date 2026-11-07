/** One row in the trace list — represents a single trace via its root span. */
export interface TraceRowDto {
  traceId:           string;
  spanId:            string;
  name:              string;
  serviceName:       string;
  services:          string[];
  status:            string;
  httpMethod:        string;
  httpPath:          string;
  httpStatusCode:    number | null;
  startTimeUnixNano: number;
  durationNanos:     number;
  spanCount:         number;
}

/** All spans within a single trace (returned by GET /api/traces/{traceId}). */
export interface SpanDto {
  traceId:           string;
  spanId:            string;
  parentSpanId:      string;
  name:              string;
  serviceName:       string;
  kind:              string;
  status:            string;
  httpStatusCode:    number | null;
  startTimeUnixNano: number;
  durationNanos:     number;
  attributes:        Record<string, string>;
}

export interface TraceQueryRequest {
  query: string;
  from?:  string;
  to?:    string;
  limit?: number;
}

/** Aggregate stats for the stats cards and the Statistics page's Traces band (GET /api/traces/stats). */
export interface TraceStatsDto {
  totalTraces:    number;
  errorRate:      number;
  p50LatencyMs:   number;
  p95LatencyMs:   number;
  p99LatencyMs:   number;
  throughputRps:  number;
  /** `buckets` equal columns of the window `from` … `to` (the server's reading of the request). */
  totalSparkline: number[];
  errorSparkline: number[];
  from:           string;
  to:             string;
  maxLatencyMs:   number;
  /** Spans and distinct services in the window. */
  spanCount:      number;
  serviceCount:   number;
  /** The window's spans by duration: 19 buckets, `boundsMs` the upper bounds of the first 18. */
  histogram:      { boundsMs: number[]; counts: number[] };
  /** The store as a whole — null when the host registers no store statistics. */
  store:          TraceStoreDto | null;
}

export interface TraceStoreDto {
  spans:         number;
  hotSpans:      number;
  segments:      number;
  fileBytes:     number;
  services:      number;
  oldestStartMs: number | null;
  newestStartMs: number | null;
}

export interface SpanQueryParams {
  from?:          string;
  to?:            string;
  service?:       string;
  spanName?:      string;
  status?:        string;
  minDurationMs?: number;
  maxDurationMs?: number;
  httpStatus?:    string;
  limit?:         number;
}

export const SPAN_STATUSES = ['Unset', 'Ok', 'Error'] as const;
export type  SpanStatus    = (typeof SPAN_STATUSES)[number];

export interface LatencyBucketDto {
  upperMs: number;
  count:   number;
}

export interface LatencyServiceDto {
  service:    string;
  spanCount:  number;
  errorCount: number;
  p50Ms:      number;
  p95Ms:      number;
  p99Ms:      number;
  p999Ms:     number;
  buckets:    LatencyBucketDto[];
}

export interface CompareTracesDto {
  traceA: SpanDto[];
  traceB: SpanDto[];
}


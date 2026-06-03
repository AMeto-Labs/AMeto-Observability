/** One row in the trace list — represents a single trace via its root span. */
export interface TraceRowDto {
  traceId:           string;
  spanId:            string;
  name:              string;
  serviceName:       string;
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
  startTimeUnixNano: number;
  durationNanos:     number;
  attributes:        Record<string, string>;
}

/** Aggregate stats for the stats cards. */
export interface TraceStatsDto {
  totalTraces:    number;
  errorRate:      number;
  p50LatencyMs:   number;
  p95LatencyMs:   number;
  throughputRps:  number;
  totalSparkline: number[];
  errorSparkline: number[];
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


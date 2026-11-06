export type MetricKind = 'Counter' | 'Gauge' | 'Histogram';

/** `value` and `sum` are null where the server has no finite number for them (NaN or ±Infinity). */
export interface MetricPointDto {
  ts:    number;
  value: number | null;
  count: number;
  sum:   number | null;
}

export interface MetricSeriesDto {
  name:   string;
  kind:   string;
  unit:   string;
  labels: Record<string, string>;
  points: MetricPointDto[];
}

/** Catalog entry — powers the Explore metric list. */
export interface MetricCatalogDto {
  name:        string;
  type:        MetricKind | string;
  unit:        string;
  labelKeys:   string[];
  cardinality: number;
  lastSeenMs:  number;
}

export type MetricAggregation =
  'none' | 'rate' | 'increase' | 'avg' | 'min' | 'max' | 'last' | 'sum' | 'quantile';

/** Body for POST /api/metrics/query. */
export interface MetricQueryRequest {
  metric:       string;
  from?:        string;
  to?:          string;
  step?:        string;          // "15s", "5m", "1h" or seconds
  aggregation?: MetricAggregation;
  quantile?:    number;          // 0..1 for aggregation = 'quantile'
  groupBy?:     string[];
  filters?:     Record<string, string>;
  topk?:        number;
}

export type MetricExprOp = 'div' | 'mul' | 'add' | 'sub';

/** Binary metric expression: left op right, optionally scaled. */
export interface MetricExprRequest {
  left:   MetricQueryRequest;
  right:  MetricQueryRequest;
  op:     MetricExprOp;
  scale?: number;
  name?:  string;
}

/** One time-step column of a histogram heatmap. */
export interface HeatmapColumnDto {
  ts:     number;
  counts: number[];
}

export interface HeatmapDto {
  bounds:  (number | null)[];  // bucket upper bounds; null for one the server cannot represent
  unit:    string;
  columns: HeatmapColumnDto[];
}

/** An exemplar: a sampled measurement linked to the trace that produced it. */
export interface ExemplarDto {
  ts:      number;             // unix nanos
  value:   number | null;      // null for NaN / ±Infinity
  traceId: string;
  spanId:  string;
  labels:  Record<string, string>;
}

// ── GET /api/metrics/stats — the Statistics page's Metrics band ──────────────

/**
 * The metric store over a window: every figure comes from the in-memory catalog and the cold
 * tier's segment list, with a segment's bytes spread over the buckets its time range covers.
 */
export interface MetricStatsDto {
  from:          string;
  to:            string;
  bucketSeconds: number;
  /** Bucket start timestamps, Unix milliseconds; every per-bucket array is aligned with it. */
  buckets:       number[];
  window:        MetricStatsWindowDto;
  store:         MetricStatsStoreDto;
  kinds:         MetricStatsKindDto[];
  /** The metrics with the most bytes written in the window, most first (at most ten). */
  byMetric:      MetricStatsMetricDto[];
  /** Every metric outside `byMetric`, folded. */
  otherBytes:    number;
  otherPoints:   number[];
}

export interface MetricStatsWindowDto {
  /** Metrics with a segment in the window, plus the hot tier's when the window reaches now. */
  metrics:         number;
  bytesWritten:    number;
  peakBucketBytes: number;
  rawBytes:        number;
  rollupBytes:     number;
}

export interface MetricStatsStoreDto {
  metrics:     number;
  series:      number;
  segments:    number;
  segmentsRaw: number;
  segments5m:  number;
  segments1h:  number;
  bytes:       number;
  bytesRaw:    number;
  bytes5m:     number;
  bytes1h:     number;
  hotSeries:   number;
  hotPoints:   number;
  hotBytes:    number;
  oldestMs:    number | null;
  newestMs:    number | null;
}

export interface MetricStatsKindDto {
  kind:    MetricKind | string;
  metrics: number;
  series:  number;
}

export interface MetricStatsMetricDto {
  name:   string;
  kind:   MetricKind | string;
  unit:   string;
  series: number;
  bytes:  number;
  points: number[];
}

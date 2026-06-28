export type MetricKind = 'Counter' | 'Gauge' | 'Histogram';

export interface MetricPointDto {
  ts:    number;
  value: number;
  count: number;
  sum:   number;
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
  bounds:  number[];           // bucket upper bounds
  unit:    string;
  columns: HeatmapColumnDto[];
}

/** An exemplar: a sampled measurement linked to the trace that produced it. */
export interface ExemplarDto {
  ts:      number;             // unix nanos
  value:   number;
  traceId: string;
  spanId:  string;
  labels:  Record<string, string>;
}

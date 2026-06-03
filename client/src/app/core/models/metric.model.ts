export interface MetricPointDto {
  ts:    number;
  value: number;
  count: number | null;
}

export interface MetricSeriesDto {
  name:   string;
  kind:   string;
  unit:   string;
  labels: Record<string, string>;
  points: MetricPointDto[];
}

export type MetricKind = 'Counter' | 'Gauge' | 'Histogram';

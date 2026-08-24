export interface ExceptionInfoDto {
  type: string;
  message?: string;
  stack?: string;
  inner?: ExceptionInfoDto;
}

export interface EventDto {
  /**
   * ISO-8601 timestamp, as a STRING. The server writes `Timestamp.ToString("O")` and
   * `JSON.parse` never produces a Date, so declaring this one was a fiction the compiler
   * believed: `ng build` failed on the one call that passed it where a string was wanted, and
   * two other call sites carried `as unknown as string` to get past it. The `date` pipe and
   * `new Date(…)` both take the string directly.
   */
  '@t': string;
  '@mt': string;
  '@l': string;
  '@x'?: ExceptionInfoDto;
  '@tr'?: string;
  '@sp'?: string;
  'service.name'?: string;
  id: string;
  props?: Record<string, unknown>;
}

export interface EventQueryResult {
  events: EventDto[];
  count: number;
  cursor?: string;
}

export interface EventQueryParams {
  filter?: string;
  from?: string;
  to?: string;
  count?: number;
  dir?: 'forward' | 'backward';
  afterId?: string;
  /** UtcTicks of the cursor event (paired with afterId). A string carries full 100 ns
   *  precision — ticks exceed Number.MAX_SAFE_INTEGER, so a number is only ms-accurate. */
  afterTs?: number | string;
  /** Comma-separated level names to filter by (omit = all levels). */
  levels?: string;
}

export interface StatsDto {
  segments: number;
  totalEvents: number;
  compressedBytes: number;
}

/** Per-service event counts bucketed over time (GET /api/events/counts). */
export interface EventCountService {
  service: string;
  count: number;
  /** One value per bucket, aligned with <see cref="EventCountsDto.buckets"/>. */
  points: number[];
}

/** Per-level event counts bucketed over time (GET /api/events/counts). */
export interface EventCountLevel {
  level: string;
  count: number;
  /** One value per bucket, aligned with <see cref="EventCountsDto.buckets"/>. */
  points: number[];
}

export interface EventCountsDto {
  from: string;
  to: string;
  bucketSeconds: number;
  total: number;
  sampled: number;
  truncated: boolean;
  /** Bucket start timestamps (unix milliseconds). */
  buckets: number[];
  services: EventCountService[];
  /**
   * Per-level breakdown. Optional for backward compatibility; the header-scan
   * backend always populates it (only levels that actually occurred are present).
   */
  levels?: EventCountLevel[];
}

/** One row of an aggregation table (GET /api/events/aggregate). */
export interface AggregationRowDto {
  /** Group key values, aligned with `keyColumns`. Null = the event carried no such value. */
  key: (string | null)[];
  /** Computed values, aligned with `valueColumns`. Null = nothing to compute from — not 0. */
  values: (number | null)[];
}

/** Answer to `select … group by …` — a table, not a list of events. */
export interface AggregationDto {
  from: string;
  to: string;
  keyColumns: string[];
  valueColumns: string[];
  rows: AggregationRowDto[];
  /** Events read to produce this. Not the number matched. */
  scanned: number;
  /** Distinct groups seen, which exceeds `rows.length` when a `limit` applied. */
  groupsFound: number;
  /**
   * The numbers are floors, not totals — the scan hit its time budget, its event budget or
   * the cap on distinct groups. Unlike a truncated list of events, which is visibly short, a
   * truncated total looks exactly like a complete one, so this has to be shown.
   */
  partial: boolean;
  /** Why, in a sentence. Present only when `partial`. */
  partialReason?: string;
}

export const LEVELS = ['Verbose', 'Debug', 'Information', 'Warning', 'Error', 'Fatal'] as const;
export type Level = (typeof LEVELS)[number];

export interface ExceptionInfoDto {
  type: string;
  message?: string;
  stack?: string;
  inner?: ExceptionInfoDto;
}

export interface EventDto {
  '@t': Date;
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
  /** UtcTicks of the cursor event (paired with afterId). */
  afterTs?: number;
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
}

export const LEVELS = ['Verbose', 'Debug', 'Information', 'Warning', 'Error', 'Fatal'] as const;
export type Level = (typeof LEVELS)[number];

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

export const LEVELS = ['Verbose', 'Debug', 'Information', 'Warning', 'Error', 'Fatal'] as const;
export type Level = (typeof LEVELS)[number];

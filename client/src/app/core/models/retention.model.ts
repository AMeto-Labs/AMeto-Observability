export interface RetentionDto {
  verboseDays: number;
  debugDays: number;
  informationDays: number;
  warningDays: number;
  errorDays: number;
  fatalDays: number;
  metricsDays: number;
  tracesDays: number;
}

export interface RetentionRunResult {
  deletedSegments: number;
  freedBytes: number;
  deletedMetricFiles: number;
  deletedTraceFiles: number;
  ranAt: string;
}

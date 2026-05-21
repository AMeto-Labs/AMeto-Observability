export interface RetentionDto {
  verboseDays: number;
  debugDays: number;
  informationDays: number;
  warningDays: number;
  errorDays: number;
  fatalDays: number;
}

export interface RetentionRunResult {
  deletedSegments: number;
  freedBytes: number;
  ranAt: string;
}

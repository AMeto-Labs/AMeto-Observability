export interface DiagnosticsDto {
  // Disk
  diskFreeBytes:  number;
  diskTotalBytes: number;

  // System RAM
  systemRamPercent: number;
  ramTargetPercent: number;

  // Process
  processWorkingSetBytes: number;
  processThreads:         number;
  processStartedAt:       string; // ISO-8601

  // Storage
  segmentCount:         number;
  totalEventCount:      number;
  totalCompressedBytes: number;
}

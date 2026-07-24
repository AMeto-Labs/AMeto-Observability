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

  // On-disk data directory (whole folder, per-signal breakdown)
  dataDirectory?:        string;
  dataTotalBytes?:       number;
  logsStorageBytes?:     number;
  metricsStorageBytes?:  number;
  tracesStorageBytes?:   number;
  databaseStorageBytes?: number;
  otherStorageBytes?:    number;
}

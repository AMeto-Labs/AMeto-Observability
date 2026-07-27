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

  // CPU. `processCpuPercent` is the last closed 30 s interval measured by the server,
  // normalised across all cores; -1 while no interval has closed yet (first ~30 s after
  // start). `processCpuSeconds` is the monotonic total, for differencing across polls.
  processCpuPercent?:     number;
  processCpuSeconds?:     number;
  processorCount?:        number;

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

  // Segment counts per signal, shown alongside each size in the breakdown.
  // `logsSegmentCount` is the engine's own figure, so it matches the Stats page.
  logsSegmentCount?:     number;
  metricsSegmentCount?:  number;
  tracesSegmentCount?:   number;
}

export type AlertSource     = 'Log' | 'Metric' | 'Trace';
export type AlertSeverity   = 'Info' | 'Warning' | 'Critical';
export type AlertComparator = 'GreaterThan' | 'GreaterOrEqual' | 'LessThan' | 'LessOrEqual';
export type AlertState      = 'Ok' | 'Pending' | 'Firing' | 'NoData';
export type TraceMetricKind = 'ErrorRatePct' | 'P50Ms' | 'P95Ms' | 'P99Ms' | 'AvgDurationMs' | 'SpanCount';

// ── Channels ────────────────────────────────────────────────────────────────
export interface AlertChannel { type: string; }
export interface WebhookChannel  extends AlertChannel { type: 'webhook';  url: string; headers?: Record<string, string>; }
export interface SmtpChannel     extends AlertChannel { type: 'smtp'; host: string; port: number; useSsl: boolean; username?: string; password?: string; from: string; to: string; }
export interface TelegramChannel extends AlertChannel { type: 'telegram'; botToken: string; chatId: string; }

// ── Rule (as returned by the API) ─────────────────────────────────────────────
export interface AlertRule {
  id:          string;
  name:        string;
  enabled:     boolean;
  severity:    AlertSeverity;
  source:      AlertSource;
  comparator:  AlertComparator;
  threshold:   number;
  window:      string;   // TimeSpan serialized
  for:         string;
  cooldown:    string;
  filter?:     string;
  noData:      boolean;
  metric?:     string;
  aggregation?: string;
  quantile?:   number;
  groupBy?:    string[];
  labels?:     Record<string, string>;
  service?:    string;
  traceMetric: TraceMetricKind;
  channels:    AlertChannel[];
  template?:   string;
}

// ── Upsert request (what the form sends) ──────────────────────────────────────
export interface AlertRuleUpsertRequest {
  id?:           string;
  name:          string;
  enabled:       boolean;
  severity:      AlertSeverity;
  source:        AlertSource;
  comparator:    AlertComparator;
  threshold:     number;
  windowSeconds: number;
  forSeconds:    number;
  cooldownSeconds: number;
  filter?:       string;
  noData?:       boolean;
  metric?:       string;
  aggregation?:  string;
  quantile?:     number;
  groupBy?:      string[];
  labels?:       Record<string, string>;
  service?:      string;
  traceMetric?:  TraceMetricKind;
  channels?:     AlertChannel[];
  template?:     string;
}

// ── Live state / history / silences ───────────────────────────────────────────
export interface AlertStateSnapshot {
  ruleId:       string;
  state:        AlertState;
  lastValue:    number;
  pendingSince?: string;
  lastFiredAt?: string;
  evaluatedAt:  string;
}

export interface AlertHistoryEntry {
  ruleId:    string;
  ruleName:  string;
  severity:  AlertSeverity;
  state:     AlertState;   // Firing or Ok (resolved)
  value:     number;
  threshold: number;
  at:        string;
}

export interface AlertSilence {
  id:        string;
  ruleId:    string;
  reason?:   string;
  until:     string;
  createdAt: string;
}

export interface AlertPreviewResult {
  value:     number;
  threshold: number;
  wouldFire: boolean;
}

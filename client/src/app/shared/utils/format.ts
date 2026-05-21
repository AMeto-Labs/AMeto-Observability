// ── Duration / TimeSpan ───────────────────────────────────────────────────────

/** Parse "hh:mm:ss" .NET TimeSpan string → total seconds. */
export function tsToSec(ts: string): number {
  const parts = (ts ?? '').split(':');
  if (parts.length !== 3) return 0;
  return (parseInt(parts[0], 10) || 0) * 3600
       + (parseInt(parts[1], 10) || 0) * 60
       + (parseInt(parts[2], 10) || 0);
}

/** Format "hh:mm:ss" TimeSpan to a human-readable string. */
export function fmtDuration(ts: string): string {
  const s = tsToSec(ts);
  if (!s) return '—';
  if (s % 3600 === 0) return `${s / 3600}h`;
  if (s % 60  === 0) return `${s / 60}m`;
  return `${Math.floor(s / 60)}m ${s % 60}s`;
}

// ── Bytes ─────────────────────────────────────────────────────────────────────

export function fmtBytes(b: number): string {
  if (b === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(Math.abs(b)) / Math.log(1024));
  const v = b / Math.pow(1024, i);
  return `${v < 10 ? v.toFixed(1) : Math.round(v)} ${units[i]}`;
}

// ── Numbers ───────────────────────────────────────────────────────────────────

export function fmtNum(n: number): string {
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`;
  if (n >= 1_000_000)     return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000)         return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

// ── Uptime ────────────────────────────────────────────────────────────────────

export function fmtUptime(startedAt: string): string {
  const ms = Date.now() - new Date(startedAt).getTime();
  const s  = Math.floor(ms / 1000);
  const d  = Math.floor(s / 86400);
  const h  = Math.floor((s % 86400) / 3600);
  const m  = Math.floor((s % 3600)  / 60);
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m`;
}

export function fmtStartedAt(startedAt: string): string {
  return new Date(startedAt).toLocaleString();
}

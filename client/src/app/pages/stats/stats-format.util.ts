import { format } from 'date-fns';

/** The numbers on the Statistics page, short enough for an 18 px tile: `2.84M`, `3.1 GB`, `812 ms`. */

/** `412`, `1.3K`, `2.84M`, `41.2M`, `1.1B`; `—` for no number. */
export function fmtCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  const abs = Math.abs(n);
  if (abs < 1000) return Number.isInteger(n) ? String(n) : n.toFixed(1);
  const units: [number, string][] = [[1e9, 'B'], [1e6, 'M'], [1e3, 'K']];
  for (const [div, unit] of units) {
    if (abs >= div) {
      const v = n / div;
      return `${Math.abs(v) >= 100 ? Math.round(v) : v.toFixed(v === Math.round(v) ? 0 : 1)}${unit}`;
    }
  }
  return String(n);
}

/** Binary units: `512 B`, `1.5 KB`, `3.1 GB`. */
export function fmtBytes(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  if (n < 1024) return `${Math.round(n)} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let v = n / 1024, i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v >= 100 ? Math.round(v) : v.toFixed(1)} ${units[i]}`;
}

/** A per-second rate: `0/s`, `0.42/s`, `1.9/s`, `13.4 rps`, `33/s`, `2.4K/s`. */
export function fmtRate(perSecond: number, unit = '/s'): string {
  if (!Number.isFinite(perSecond) || perSecond <= 0) return `0${unit}`;
  if (perSecond < 10) return `${perSecond.toFixed(perSecond < 1 ? 2 : 1)}${unit}`;
  if (perSecond < 100) {
    const rounded = Math.round(perSecond * 10) / 10;
    return `${Number.isInteger(rounded) ? rounded : rounded.toFixed(1)}${unit}`;
  }
  return `${fmtCount(Math.round(perSecond))}${unit}`;
}

/** `1.9 %`; a share of a hundred, with the space the design puts before the sign. */
export function fmtPct(p: number | null | undefined, digits = 1): string {
  return p == null || !Number.isFinite(p) ? '—' : `${p.toFixed(digits)} %`;
}

/** A duration in ms: `0.40 ms`, `4.6 ms`, `46 ms`, `1.20 s`, `12.4 s`, `2.1 min`. */
export function fmtMs(ms: number | null | undefined): string {
  if (ms == null || !Number.isFinite(ms)) return '—';
  if (ms < 1) return `${ms.toFixed(2)} ms`;
  if (ms < 10) return `${ms.toFixed(1)} ms`;
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(ms < 10_000 ? 2 : 1)} s`;
  return `${(ms / 60_000).toFixed(1)} min`;
}

/** `29 Sep`, with the year only when it is not the current one; `—` for no date. */
export function fmtDateMs(ms: number | null | undefined, now = new Date()): string {
  if (ms == null || !Number.isFinite(ms)) return '—';
  const d = new Date(ms);
  return format(d, d.getFullYear() === now.getFullYear() ? 'd MMM' : 'd MMM yyyy');
}

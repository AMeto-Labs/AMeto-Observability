import { Params } from '@angular/router';
import { addDays, format, startOfDay } from 'date-fns';

import {
  DEFAULT_PRESET, QUICK_RANGE_LABELS, TimePreset, endOfMinuteIso, fmtDateInput, fmtPoint, isQuickRange,
  parseCustomDate, quickRangeStart, utcOffsetLabel,
} from '../../shared/utils/time-range';

/** The three bands of the page, in the order it stacks them (and the phone's tabs). */
export type StatsSignal = 'logs' | 'metrics' | 'traces';
export const STATS_SIGNALS: readonly StatsSignal[] = ['logs', 'metrics', 'traces'];

export function isStatsSignal(raw: string | null | undefined): raw is StatsSignal {
  return raw === 'logs' || raw === 'metrics' || raw === 'traces';
}

/** Where each band's "Open …" and bar clicks land. */
export const SIGNAL_ROUTE: Readonly<Record<StatsSignal, string>> = {
  logs: '/events', metrics: '/metrics', traces: '/traces',
};

/**
 * The step table, shared with the server (`MetricStoreStats.AutoBucketSeconds`): the smallest
 * step that keeps a window under {@link MAX_BUCKETS} columns. 1 d → 15 min (96), 3 d → 1 h (72),
 * 7 d → 2 h (84), 14 d → 4 h (84), a month → 12 h (~60), 3 months → 1 day (~92).
 */
export const STEP_SECONDS: readonly number[] =
  [60, 300, 900, 1800, 3600, 7200, 14_400, 21_600, 43_200, 86_400, 172_800, 604_800];
export const MAX_BUCKETS = 96;

export function bucketSecondsFor(spanMs: number): number {
  const raw = Math.max(1, spanMs) / 1000 / MAX_BUCKETS;
  for (const s of STEP_SECONDS) if (s >= raw) return s;
  return Math.ceil(raw / 604_800) * 604_800;
}

/** The quick ranges counted in days, for the step: the months take their calendar span, which lands on 12 h or 1 day either way. */
const NOMINAL_DAYS: Partial<Record<TimePreset, number>> = { '1d': 1, '3d': 3, '7d': 7, '14d': 14 };

/**
 * The window the three bands share, resolved once against the clock. The grid is the bucket
 * axis every chart draws on: bucket starts aligned to the step from the epoch (as the server
 * aligns the log counts), the first holding `from` and the last holding `to`.
 */
export interface StatsWindow {
  preset: TimePreset;
  fromMs: number;
  toMs: number;
  /** True when `to` is the present: the window moves with the clock and the hot tiers count. */
  openEnded: boolean;
  bucketSeconds: number;
  gridFromMs: number;
  /** The exclusive end of the last bucket. */
  gridEndMs: number;
  buckets: number;
}

/**
 * A preset as of `now`, or a custom window from the date inputs' strings; null when the custom
 * bounds do not make a window (no start, an unreadable bound, an end before the start) or one the
 * server would take (a start before 1970 — its arithmetic counts from there — or an end more than
 * a day past now: a clock ahead of the server's is tolerated, a typo in the year is not).
 */
export function resolveWindow(preset: TimePreset, customFrom: string, customTo: string, now = new Date()): StatsWindow | null {
  let from: Date;
  let to: Date;
  let openEnded: boolean;
  if (preset === 'custom') {
    const start = parseCustomDate(customFrom);
    if (!start) return null;
    const end = customTo ? parseCustomDate(customTo) : null;
    if (customTo && !end) return null;
    from = start;
    openEnded = !end;
    // A To bound is a minute and the whole minute is in the window (see endOfMinuteIso).
    to = end ? new Date(Math.floor(end.getTime() / 60_000) * 60_000 + 59_999) : now;
    if (to.getTime() <= from.getTime()) return null;
    if (from.getTime() < 0 || to.getTime() > now.getTime() + DAY) return null;
  } else {
    from = quickRangeStart(preset, now);
    to = now;
    openEnded = true;
  }
  // The step from the window's nominal span: a day is 24 h even on the day a DST change makes
  // 23 or 25 hours long — 25 h / 96 would give the one day a year 30-minute buckets.
  const nominalDays = preset === 'custom' ? undefined : NOMINAL_DAYS[preset];
  const bucketSeconds = bucketSecondsFor(nominalDays ? nominalDays * DAY : to.getTime() - from.getTime());
  const stepMs = bucketSeconds * 1000;
  const first = Math.floor(from.getTime() / stepMs);
  const last = Math.floor(to.getTime() / stepMs);
  return {
    preset, fromMs: from.getTime(), toMs: to.getTime(), openEnded, bucketSeconds,
    gridFromMs: first * stepMs, gridEndMs: (last + 1) * stepMs, buckets: last - first + 1,
  };
}

/**
 * The bounds a band's request carries: `from` on the grid, so the server's buckets are the
 * page's; `to` only for a closed window — an open one leaves the end to the server's own "now",
 * so a clock ahead or behind the browser's cuts nothing off (as the Logs page does).
 */
export function requestBounds(w: StatsWindow): { from: string; to?: string } {
  return {
    from: new Date(w.gridFromMs).toISOString(),
    to: w.openEnded ? undefined : endOfMinuteIso(new Date(w.toMs)),
  };
}

/** `15 min`, `2 h`, `1 day`, `7 days`. */
export function fmtBucket(seconds: number): string {
  if (seconds % 86_400 === 0) { const d = seconds / 86_400; return d === 1 ? '1 day' : `${d} days`; }
  if (seconds % 3600 === 0) return `${seconds / 3600} h`;
  return `${Math.round(seconds / 60)} min`;
}

/**
 * The page's subtitle: `30 Oct 14:22 → now · 84 buckets of 2 h · UTC+05:00`. The grid is aligned
 * to the epoch, so a bucket of 12 h or more is a UTC half-day or day, not a local one — a "day"
 * bar in UTC+05:00 runs 05:00 → 05:00 — and the subtitle says so where it matters.
 */
export function windowLabel(w: StatsWindow, now = new Date()): string {
  const from = fmtPoint(new Date(w.fromMs), now);
  const to = w.openEnded ? 'now' : fmtPoint(new Date(w.toMs), now);
  const utcDays = w.bucketSeconds >= 43_200 ? ' (UTC days)' : '';
  return `${from} → ${to} · ${w.buckets} buckets of ${fmtBucket(w.bucketSeconds)}${utcDays} · ${utcOffsetLabel(now)}`;
}

/** The tiles' caption: `Last 7 days`, or `12 – 16 Oct` for a custom window. */
export function windowCaption(w: StatsWindow, now = new Date()): string {
  if (w.preset !== 'custom') return QUICK_RANGE_LABELS[w.preset];
  const from = new Date(w.fromMs), to = new Date(w.toMs);
  if (w.openEnded) return `Since ${fmtDay(from, now)}`;
  if (from.getMonth() === to.getMonth() && from.getFullYear() === to.getFullYear())
    return `${format(from, 'd')} – ${fmtDay(to, now)}`;
  return `${fmtDay(from, now)} – ${fmtDay(to, now)}`;
}

function fmtDay(d: Date, now: Date): string {
  return format(d, d.getFullYear() === now.getFullYear() ? 'd MMM' : 'd MMM yyyy');
}

export interface AxisTick { pct: number; label: string; }

const HOUR = 3_600_000;
const DAY = 86_400_000;
const TICK_STEPS = [HOUR, 2 * HOUR, 3 * HOUR, 4 * HOUR, 6 * HOUR, 12 * HOUR, DAY, 2 * DAY, 7 * DAY, 14 * DAY, 30 * DAY];

/**
 * Up to about eight labels under a chart, on the clock rather than on the buckets: hour marks
 * (`HH:mm`, the date at midnight) for a window of days, dates for a window of weeks. Nothing in
 * the outer 1.5 % of the axis, where a label would be clipped.
 */
export function axisTicks(gridFromMs: number, gridEndMs: number, now = new Date(), maxTicks = 8): AxisTick[] {
  const span = gridEndMs - gridFromMs;
  if (span <= 0) return [];
  const step = TICK_STEPS.find(s => span / s <= maxTicks) ?? Math.ceil(span / maxTicks / DAY) * DAY;
  const advance = (t: Date) => step < DAY ? addLocalHours(t, step / HOUR) : addDays(t, step / DAY);
  // From the local midnight before the start, in steps, so the marks fall on 00:00, 04:00, 08:00 …
  let t = startOfDay(new Date(gridFromMs));
  while (t.getTime() < gridFromMs) t = advance(t);
  const ticks: AxisTick[] = [];
  for (; t.getTime() < gridEndMs; t = advance(t)) {
    const pct = ((t.getTime() - gridFromMs) / span) * 100;
    if (pct < 1.5 || pct > 98.5) continue;
    const midnight = t.getHours() === 0 && t.getMinutes() === 0;
    ticks.push({ pct, label: step < DAY && !midnight ? format(t, 'HH:mm') : fmtDay(t, now) });
  }
  return ticks;
}

/**
 * `t` plus `hours` on the local clock, not on the timeline: midnight plus four is 04:00 on the day
 * a DST change happens too, where four absolute hours would read 03:00 or 05:00 from then on.
 */
function addLocalHours(t: Date, hours: number): Date {
  const next = new Date(t);
  next.setHours(t.getHours() + hours);
  return next;
}

/** The URL's `preset`, `from`, `to` and `tab` as state; the defaults for anything it cannot read. */
export function parseStatsParams(
  preset: string | null, from: string | null, to: string | null, tab: string | null,
): { preset: TimePreset; customFrom: string; customTo: string; tab: StatsSignal } {
  const customFrom = from ?? '';
  const customTo = to ?? '';
  const custom = preset === 'custom' && resolveWindow('custom', customFrom, customTo) !== null;
  return {
    preset: custom ? 'custom' : isQuickRange(preset) ? preset : DEFAULT_PRESET,
    customFrom: custom ? customFrom : '',
    customTo: custom ? customTo : '',
    tab: isStatsSignal(tab) ? tab : 'logs',
  };
}

/** Where "Open Logs / Metrics / Traces" lands: the same window, in each page's own URL words. */
export function pageLinkParams(signal: StatsSignal, w: StatsWindow): Params {
  const from = new Date(w.fromMs), to = new Date(w.toMs);
  if (signal === 'logs') {
    if (w.preset === '1d' || w.preset === '3d' || w.preset === '7d') return { preset: w.preset };
    return { preset: 'custom', from: fmtDateInput(from), to: w.openEnded ? null : fmtDateInput(to) };
  }
  // Traces and Metrics share one vocabulary: `range` (15m … 24h) with datetime-local custom bounds.
  if (w.preset === '1d') return { range: '24h' };
  return { range: 'custom', cfrom: toLocalInput(from), cto: w.openEnded ? null : toLocalInput(to) };
}

/** A bar's bucket `[start, end)` as the page's custom window. */
export function bucketLinkParams(signal: StatsSignal, startMs: number, endMs: number): Params {
  if (signal === 'logs') {
    // The Logs page's To is a minute, inclusive to its last tick: the bucket's last minute.
    const to = Math.max(startMs, endMs - 60_000);
    return { preset: 'custom', from: fmtDateInput(new Date(startMs)), to: fmtDateInput(new Date(to)) };
  }
  return { range: 'custom', cfrom: toLocalInput(new Date(startMs)), cto: toLocalInput(new Date(endMs)) };
}

function toLocalInput(d: Date): string {
  return format(d, "yyyy-MM-dd'T'HH:mm");
}

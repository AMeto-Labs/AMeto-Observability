import { format, isSameDay, subDays, subMonths } from 'date-fns';

/**
 * The time range every page shares: the quick ranges, where each starts as of now, the date
 * inputs' format, and how a window reads on a button. The Logs page offers the first three quick
 * ranges and the Statistics page all six; both open the same popover
 * (shared/components/date-range-dropdown) and resolve a range with the same functions, so a
 * window means one thing everywhere.
 */

/** The quick ranges, in the order every control lists them. */
export type QuickRange = '1d' | '3d' | '7d' | '14d' | '1m' | '3m';
export type TimePreset = QuickRange | 'custom';

export const QUICK_RANGES: readonly QuickRange[] = ['1d', '3d', '7d', '14d', '1m', '3m'];

export const QUICK_RANGE_LABELS: Readonly<Record<QuickRange, string>> = {
  '1d':  'Last 1 day',
  '3d':  'Last 3 days',
  '7d':  'Last 7 days',
  '14d': 'Last 14 days',
  '1m':  'Last month',
  '3m':  'Last 3 months',
};

/** The window a fresh page opens on, and the one Reset and Clear return to. */
export const DEFAULT_PRESET: TimePreset = '1d';

/** True for a string naming a quick range. Own keys only: `in` would accept `toString`. */
export function isQuickRange(raw: string | null | undefined): raw is QuickRange {
  return raw != null && Object.hasOwn(QUICK_RANGE_LABELS, raw);
}

/**
 * Where a quick range starts as of `now`: N calendar days back, or N calendar months back, to
 * the minute. Calendar units, so a DST switch inside the window moves no bound by an hour, and
 * "last month" ends on the same day of the previous month rather than thirty days ago.
 */
export function quickRangeStart(range: QuickRange, now = new Date()): Date {
  switch (range) {
    case '1d':  return subDays(now, 1);
    case '3d':  return subDays(now, 3);
    case '7d':  return subDays(now, 7);
    case '14d': return subDays(now, 14);
    case '1m':  return subMonths(now, 1);
    case '3m':  return subMonths(now, 3);
  }
}

export function fmtDateInput(d: Date): string {
  return format(d, 'yyyy-MM-dd HH:mm');
}

/** `yyyy-MM-dd HH:mm` (the stores') → `yyyy-MM-ddTHH:mm` (a datetime-local input's), and back. */
export function toDateTimeLocal(v: string): string {
  return v ? v.replace(' ', 'T').slice(0, 16) : '';
}
export function fromDateTimeLocal(v: string): string {
  return v ? v.replace('T', ' ').slice(0, 16) : '';
}

/**
 * The last instant of `d`'s minute as an ISO-8601 UTC string with seven fractional digits —
 * the server's 100 ns tick. A To bound is a minute and the whole minute is in the window, while
 * the server's `to` is inclusive (API.md): sending the next minute would also take in events
 * stamped exactly on it, so this sends the tick before it. `2026-10-18 23:59` becomes
 * `…T18:59:59.9999999Z` at UTC+5: everything up to, and nothing at, 2026-10-19 00:00.
 */
export function endOfMinuteIso(d: Date): string {
  const nextMinute = Math.floor(d.getTime() / 60_000) * 60_000 + 60_000;
  // toISOString stops at milliseconds (…59.999Z); the four 9s after it are the sub-ms ticks.
  return new Date(nextMinute - 1).toISOString().replace(/Z$/, '9999Z');
}

/** Parses `yyyy-MM-dd [HH:mm]` (also accepts legacy `dd/MM/yyyy [HH:mm]`). */
export function parseCustomDate(val: string): Date | null {
  if (!val) return null;
  const iso = val.match(/^(\d{4})-(\d{2})-(\d{2})(?:[\s,T]+(\d{1,2}):(\d{2}))?$/);
  if (iso) {
    const [, y, mo, d, h = '0', min = '0'] = iso;
    const dt = new Date(+y, +mo - 1, +d, +h, +min);
    return isNaN(dt.getTime()) ? null : dt;
  }
  const leg = val.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:[\s,T]+(\d{1,2}):(\d{2}))?$/);
  if (leg) {
    const [, d, mo, y, h = '0', min = '0'] = leg;
    const dt = new Date(+y, +mo - 1, +d, +h, +min);
    return isNaN(dt.getTime()) ? null : dt;
  }
  return null;
}

/**
 * A preset's bounds as of now, in the date inputs' format: the quick range's start
 * ({@link quickRangeStart}) up to now (`to` ''). `custom` has no bounds of its own.
 */
export function presetRange(preset: TimePreset, now = new Date()): { from: string; to: string } {
  if (preset === 'custom') return { from: '', to: '' };
  return { from: fmtDateInput(quickRangeStart(preset, now)), to: '' };
}

/** A duration in its two largest units, rounded to the minute: `45 m`, `4 h`, `4 d 9 h`. */
export function fmtSpan(ms: number): string {
  const min = Math.max(0, Math.round(ms / 60_000));
  if (min < 60) return `${min} m`;
  const h = Math.floor(min / 60);
  if (h < 24) return min % 60 ? `${h} h ${min % 60} m` : `${h} h`;
  const d = Math.floor(h / 24);
  return h % 24 ? `${d} d ${h % 24} h` : `${d} d`;
}

/** `17 Oct 09:00`, with the year only when it is not the current one. */
export function fmtPoint(d: Date, now: Date): string {
  return format(d, d.getFullYear() === now.getFullYear() ? 'd MMM HH:mm' : 'd MMM yyyy HH:mm');
}

/**
 * How a custom window reads on its button: `17 Oct 09:00 → 13:00` plus its length when both ends
 * are set (the date once when they share a day), `Since 17 Oct 09:00` when the end is open.
 * `extra` is the length, drawn quieter than the window itself.
 */
export function customRangeLabel(from: Date | null, to: Date | null, now = new Date()): { text: string; extra: string } {
  if (!from) return { text: 'Custom', extra: '' };
  if (!to) return { text: `Since ${fmtPoint(from, now)}`, extra: '' };
  const end = isSameDay(from, to) ? format(to, 'HH:mm') : fmtPoint(to, now);
  return { text: `${fmtPoint(from, now)} → ${end}`, extra: fmtSpan(to.getTime() - from.getTime()) };
}

/** The browser's offset from UTC as `UTC+05:00` — the zone every time on the page is in. */
export function utcOffsetLabel(d = new Date()): string {
  const off = -d.getTimezoneOffset();
  const abs = Math.abs(off);
  const hh = String(Math.floor(abs / 60)).padStart(2, '0');
  const mm = String(abs % 60).padStart(2, '0');
  return `UTC${off < 0 ? '−' : '+'}${hh}:${mm}`;
}

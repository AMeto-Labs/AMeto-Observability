import { describe, it, expect } from 'vitest';

import {
  axisTicks, bucketLinkParams, bucketSecondsFor, fmtBucket, pageLinkParams, parseStatsParams, requestBounds,
  resolveWindow, windowCaption, windowLabel,
} from './stats-range.util';
import { quickRangeStart } from '../../shared/utils/time-range';

/** 2026-11-06 14:22:30 local, a Friday: the clock every window here is resolved against. */
const NOW = new Date(2026, 10, 6, 14, 22, 30);
const local = (y: number, mo: number, d: number, h = 0, mi = 0) => new Date(y, mo - 1, d, h, mi);

describe('the bucket step', () => {
  it('follows the step table the server shares: about 96 columns at most', () => {
    const day = 86_400_000;
    expect(bucketSecondsFor(day)).toBe(900);
    expect(bucketSecondsFor(3 * day)).toBe(3600);
    expect(bucketSecondsFor(7 * day)).toBe(7200);
    expect(bucketSecondsFor(14 * day)).toBe(14_400);
    expect(bucketSecondsFor(30 * day)).toBe(43_200);
    expect(bucketSecondsFor(92 * day)).toBe(86_400);
    expect(bucketSecondsFor(2 * 3_600_000)).toBe(300);
    expect(bucketSecondsFor(400 * day)).toBe(604_800);
  });

  it('names a step the way the captions do', () => {
    expect(fmtBucket(900)).toBe('15 min');
    expect(fmtBucket(7200)).toBe('2 h');
    expect(fmtBucket(86_400)).toBe('1 day');
    expect(fmtBucket(172_800)).toBe('2 days');
  });
});

describe('resolving the window', () => {
  it('puts a quick range on the grid: the first bucket holds the start, the last holds now', () => {
    const w = resolveWindow('7d', '', '', NOW)!;
    expect(w.preset).toBe('7d');
    expect(new Date(w.fromMs)).toEqual(new Date(2026, 9, 30, 14, 22, 30));
    expect(w.fromMs).toBe(quickRangeStart('7d', NOW).getTime());
    expect(w.toMs).toBe(NOW.getTime());
    expect(w.openEnded).toBe(true);
    expect(w.bucketSeconds).toBe(7200);
    expect(w.gridFromMs % 7_200_000).toBe(0);
    expect(w.gridFromMs).toBeLessThanOrEqual(w.fromMs);
    expect(w.gridEndMs).toBeGreaterThan(w.toMs);
    expect(w.gridEndMs - w.gridFromMs).toBe(w.buckets * 7_200_000);
    // Seven days of two-hour steps plus the partial bucket at each end.
    expect(w.buckets).toBe(85);
  });

  it('takes a month and three months as calendar months, a day as 15-minute buckets', () => {
    expect(new Date(resolveWindow('1m', '', '', NOW)!.fromMs)).toEqual(new Date(2026, 9, 6, 14, 22, 30));
    expect(new Date(resolveWindow('3m', '', '', NOW)!.fromMs)).toEqual(new Date(2026, 7, 6, 14, 22, 30));
    expect(resolveWindow('3m', '', '', NOW)!.bucketSeconds).toBe(86_400);
    expect(resolveWindow('1d', '', '', NOW)!.bucketSeconds).toBe(900);
  });

  it('reads a custom window from the inputs, the end running to the last tick of its minute', () => {
    const w = resolveWindow('custom', '2026-10-12 09:00', '2026-10-16 18:30', NOW)!;
    expect(new Date(w.fromMs)).toEqual(local(2026, 10, 12, 9, 0));
    expect(w.toMs).toBe(local(2026, 10, 16, 18, 30).getTime() + 59_999);
    expect(w.openEnded).toBe(false);
    expect(w.bucketSeconds).toBe(7200);
    expect(requestBounds(w).to).toMatch(/9999Z$/);
  });

  it('leaves an open custom end to the server and asks for the grid start', () => {
    const w = resolveWindow('custom', '2026-11-06 09:03', '', NOW)!;
    expect(w.openEnded).toBe(true);
    expect(w.toMs).toBe(NOW.getTime());
    const bounds = requestBounds(w);
    expect(bounds.to).toBeUndefined();
    expect(Date.parse(bounds.from)).toBe(w.gridFromMs);
    expect(w.gridFromMs).toBeLessThanOrEqual(local(2026, 11, 6, 9, 3).getTime());
  });

  it('refuses a custom window with no start, an unreadable bound or an end before its start', () => {
    expect(resolveWindow('custom', '', '', NOW)).toBeNull();
    expect(resolveWindow('custom', 'yesterday', '', NOW)).toBeNull();
    expect(resolveWindow('custom', '2026-10-12 09:00', 'soon', NOW)).toBeNull();
    expect(resolveWindow('custom', '2026-10-16 09:00', '2026-10-12 09:00', NOW)).toBeNull();
  });

  it('labels the window as the subtitle and the tiles read it', () => {
    const quick = resolveWindow('7d', '', '', NOW)!;
    expect(windowLabel(quick, NOW)).toMatch(/^30 Oct 14:22 → now · 85 buckets of 2 h · UTC/);
    expect(windowCaption(quick, NOW)).toBe('Last 7 days');

    // Five days: 2-hour buckets (an hour would be 120 columns, past the 96 the table allows).
    const custom = resolveWindow('custom', '2026-10-12 00:00', '2026-10-16 23:59', NOW)!;
    expect(windowLabel(custom, NOW)).toMatch(/^12 Oct 00:00 → 16 Oct 23:59 · 61 buckets of 2 h · UTC/);
    expect(windowCaption(custom, NOW)).toBe('12 – 16 Oct');
    expect(windowCaption(resolveWindow('custom', '2026-09-28 00:00', '2026-10-02 00:00', NOW)!, NOW)).toBe('28 Sep – 2 Oct');
    expect(windowCaption(resolveWindow('custom', '2026-11-01 00:00', '', NOW)!, NOW)).toBe('Since 1 Nov');
  });
});

describe('the axis', () => {
  it('marks a day in hours, naming midnight by its date, and a week in days', () => {
    const day = resolveWindow('1d', '', '', NOW)!;
    const hours = axisTicks(day.gridFromMs, day.gridEndMs, NOW);
    expect(hours.length).toBeGreaterThanOrEqual(5);
    expect(hours.map(t => t.label)).toContain('6 Nov');
    expect(hours.filter(t => /^\d\d:\d\d$/.test(t.label)).length).toBeGreaterThanOrEqual(4);
    expect(hours.every(t => t.pct > 1.5 && t.pct < 98.5)).toBe(true);

    const week = resolveWindow('7d', '', '', NOW)!;
    const days = axisTicks(week.gridFromMs, week.gridEndMs, NOW).map(t => t.label);
    expect(days).toEqual(['31 Oct', '1 Nov', '2 Nov', '3 Nov', '4 Nov', '5 Nov', '6 Nov']);
  });

  it('keeps a quarter to about eight labels', () => {
    const quarter = resolveWindow('3m', '', '', NOW)!;
    const ticks = axisTicks(quarter.gridFromMs, quarter.gridEndMs, NOW);
    expect(ticks.length).toBeGreaterThanOrEqual(5);
    expect(ticks.length).toBeLessThanOrEqual(8);
  });
});

describe('the URL and the links out', () => {
  it('reads the query string, falling back to the defaults for what it cannot read', () => {
    expect(parseStatsParams('7d', null, null, 'traces')).toEqual({ preset: '7d', customFrom: '', customTo: '', tab: 'traces' });
    expect(parseStatsParams('custom', '2026-10-12 09:00', '2026-10-16 18:30', null))
      .toEqual({ preset: 'custom', customFrom: '2026-10-12 09:00', customTo: '2026-10-16 18:30', tab: 'logs' });
    expect(parseStatsParams('custom', 'garbage', null, 'nope')).toEqual({ preset: '1d', customFrom: '', customTo: '', tab: 'logs' });
    expect(parseStatsParams('30d', null, null, null).preset).toBe('1d');
    expect(parseStatsParams(null, null, null, null).preset).toBe('1d');
  });

  it("hands each page the window in its own words", () => {
    const week = resolveWindow('7d', '', '', NOW)!;
    expect(pageLinkParams('logs', week)).toEqual({ preset: '7d' });
    expect(pageLinkParams('traces', week)).toEqual({ range: 'custom', cfrom: '2026-10-30T14:22', cto: null });

    const day = resolveWindow('1d', '', '', NOW)!;
    expect(pageLinkParams('metrics', day)).toEqual({ range: '24h' });

    const month = resolveWindow('1m', '', '', NOW)!;
    expect(pageLinkParams('logs', month)).toEqual({ preset: 'custom', from: '2026-10-06 14:22', to: null });

    const custom = resolveWindow('custom', '2026-10-12 09:00', '2026-10-16 18:30', NOW)!;
    expect(pageLinkParams('logs', custom)).toEqual({ preset: 'custom', from: '2026-10-12 09:00', to: '2026-10-16 18:30' });
    expect(pageLinkParams('metrics', custom)).toEqual({ range: 'custom', cfrom: '2026-10-12T09:00', cto: '2026-10-16T18:30' });
  });

  it('opens a bar as the bucket it stands for', () => {
    const start = local(2026, 11, 6, 12, 0).getTime();
    const end = local(2026, 11, 6, 14, 0).getTime();
    expect(bucketLinkParams('logs', start, end)).toEqual({ preset: 'custom', from: '2026-11-06 12:00', to: '2026-11-06 13:59' });
    expect(bucketLinkParams('traces', start, end)).toEqual({ range: 'custom', cfrom: '2026-11-06T12:00', cto: '2026-11-06T14:00' });
  });
});

import { TestBed } from '@angular/core/testing';
import { provideZonelessChangeDetection } from '@angular/core';
import { Router, provideRouter } from '@angular/router';
import { NEVER, of } from 'rxjs';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

import { EventsStore } from './store/events.store';
import { ApiService } from '../../core/services/api.service';
import { customRangeLabel, fmtSpan, parsePreset } from './store/events-filter.util';

/**
 * The time range in the filter bar: inline presets that stay relative, earlier / later / zoom
 * out over whatever window is set, and Live beside them.
 *
 * <p>Only `Date` is faked. Zoneless Angular schedules change detection on setTimeout, so faking
 * timers wholesale is a trap this repo already avoids (see events.live-batching.spec.ts).</p>
 */

/** 2026-10-17 11:42:30 local — a Saturday, mid-morning, so no window here crosses midnight. */
const NOW = new Date(2026, 9, 17, 11, 42, 30);
const at = (h: number, m: number, day = 17) => new Date(2026, 9, day, h, m);

describe('time range labels', () => {
  it('reads a legacy 1d link as 24h and refuses names it does not know', () => {
    expect(parsePreset('1d')).toBe('24h');
    expect(parsePreset('15m')).toBe('15m');
    expect(parsePreset('custom')).toBe('custom');
    expect(parsePreset('toString')).toBeNull();
    expect(parsePreset(null)).toBeNull();
  });

  it('gives a length in its two largest units', () => {
    expect(fmtSpan(45 * 60_000)).toBe('45 m');
    expect(fmtSpan(4 * 3_600_000)).toBe('4 h');
    expect(fmtSpan(4.5 * 3_600_000)).toBe('4 h 30 m');
    expect(fmtSpan((4 * 24 + 9) * 3_600_000 + 30 * 60_000)).toBe('4 d 9 h');
  });

  it('names a custom window the way the button shows it', () => {
    expect(customRangeLabel(at(9, 0), at(13, 0), NOW)).toEqual({ text: '17 Oct 09:00 → 13:00', extra: '4 h' });
    expect(customRangeLabel(at(9, 0, 12), at(18, 30, 16), NOW))
      .toEqual({ text: '12 Oct 09:00 → 16 Oct 18:30', extra: '4 d 9 h' });
    expect(customRangeLabel(at(9, 0), null, NOW)).toEqual({ text: 'Since 17 Oct 09:00', extra: '' });
    expect(customRangeLabel(new Date(2025, 11, 31, 22, 0), null, NOW).text).toBe('Since 31 Dec 2025 22:00');
  });
});

describe('Events time range — presets, earlier / later, zoom out', () => {
  let store: InstanceType<typeof EventsStore>;
  let searches: { from?: string; to?: string }[];

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(NOW);
    searches = [];

    const api: Partial<ApiService> = {
      streamEvents: ((q: { from?: string; to?: string }) => {
        searches.push({ from: q.from, to: q.to });
        return NEVER;
      }) as unknown as ApiService['streamEvents'],
      streamLive: () => NEVER,
      aggregate: () => NEVER,
      getServiceNames: () => of([]),
      recordSearch: () => of(undefined as void),
    };

    TestBed.configureTestingModule({
      providers: [
        provideZonelessChangeDetection(),
        provideRouter([]),
        { provide: ApiService, useValue: api },
        EventsStore,
      ],
    });
    store = TestBed.inject(EventsStore);
  });

  afterEach(() => {
    TestBed.resetTestingModule();
    vi.useRealTimers();
  });

  const lastSearch = () => searches[searches.length - 1];

  it('resolves a preset against the clock on every load, not once when it was picked', () => {
    store.setTimePreset('15m');
    expect(store.customFrom()).toBe('2026-10-17 11:27');

    vi.setSystemTime(new Date(NOW.getTime() + 20 * 60_000));
    store.search();

    expect(store.customFrom()).toBe('2026-10-17 11:47');
    expect(lastSearch().from).toBe(at(11, 47).toISOString());
    expect(lastSearch().to).toBeUndefined();
  });

  it('zoom out climbs the inline presets, so the window stays relative', () => {
    store.setTimePreset('15m');
    store.zoomOut();
    expect(store.timePreset()).toBe('1h');
    expect(store.customTo()).toBe('');
  });

  it('zoom out past the last preset doubles the window and keeps it ending now', () => {
    store.setTimePreset('4h');
    store.zoomOut();
    store.zoomOut();                                            // 24h → 7d is the last step
    expect(store.timePreset()).toBe('7d');
    store.zoomOut();
    expect(store.timePreset()).toBe('custom');
    expect(store.customTo()).toBe('');
    // 7d is day-aligned: from 10 Oct 00:00, so the span is 7 d 11 h 43 m and doubling it reaches
    // back 14 d 23 h 26 m from now.
    expect(store.customFrom()).toBe('2026-10-02 12:16');
  });

  it('zoom out on a closed window doubles it around its middle', () => {
    store.setTimeRange(at(9, 0).toISOString(), at(10, 0).toISOString());
    store.zoomOut();
    expect(store.customFrom()).toBe('2026-10-17 08:30');
    expect(store.customTo()).toBe('2026-10-17 10:30');
  });

  it('earlier moves the window back by its own length and closes it', () => {
    store.setTimePreset('15m');                                 // 11:27 → now (11:42:30)
    store.shiftWindow(-1);
    expect(store.timePreset()).toBe('custom');
    expect(store.customTo()).toBe('2026-10-17 11:27');
    expect(store.customFrom()).toBe('2026-10-17 11:11');        // 16 whole minutes back
    expect(lastSearch().to).toBe(at(11, 27).toISOString());
  });

  it('later does nothing on a window that already ends now', () => {
    store.setTimePreset('15m');
    const before = searches.length;
    expect(store.canShiftLater()).toBe(false);
    store.shiftWindow(1);
    expect(searches.length).toBe(before);
    expect(store.timePreset()).toBe('15m');
  });

  it('later moves a closed window forward, and ends at now rather than past it', () => {
    store.setTimeRange(at(9, 0).toISOString(), at(10, 0).toISOString());
    store.shiftWindow(1);
    expect(store.customFrom()).toBe('2026-10-17 10:00');
    expect(store.customTo()).toBe('2026-10-17 11:00');

    store.shiftWindow(1);                                       // 11:00 → 12:00 would pass now
    expect(store.customTo()).toBe('');
    expect(store.customFrom()).toBe('2026-10-17 10:42');        // one hour back from 11:42:30
  });

  it('picking a preset while the tail runs stops the tail and searches the window', () => {
    store.toggleLive();
    expect(store.live()).toBe(true);
    const before = searches.length;

    store.setTimePreset('1h');

    expect(store.live()).toBe(false);
    expect(searches.length).toBe(before + 1);
    expect(lastSearch().from).toBe(at(10, 42).toISOString());
    // The list now answers the window, so it is not "paused" any more.
    expect(store.liveFrozenAt()).toBeNull();
  });

  it('a paused tail says when it froze, until something replaces the list', () => {
    store.setTimePreset('15m');
    store.toggleLive();
    store.toggleLive();
    expect(store.liveFrozenAt()).toBe(NOW.getTime());
    expect(store.windowReadout()).toBe('');

    store.search();
    expect(store.liveFrozenAt()).toBeNull();
  });

  it('labels a preset by name and a custom window by its bounds', () => {
    store.setTimePreset('4h');
    expect(store.rangeLabel()).toEqual({ text: 'Last 4 hours', extra: '' });
    expect(store.windowReadout()).toBe('07:42 → now');

    store.setTimeRange(at(9, 0).toISOString(), at(13, 0).toISOString());
    expect(store.rangeLabel()).toEqual({ text: '17 Oct 09:00 → 13:00', extra: '4 h' });
    expect(store.windowReadout()).toBe('');
  });

  describe('opening from a link', () => {
    const open = async (url: string) => {
      await TestBed.inject(Router).navigateByUrl(url);
      store.initFromUrl();
    };

    it('opens a preset from now, whatever start the link was made with', async () => {
      await open('/?preset=15m&from=2026-10-01%2009:00');
      expect(store.timePreset()).toBe('15m');
      expect(store.customFrom()).toBe('2026-10-17 11:27');
    });

    it('reads the old 1d default as 24h', async () => {
      await open('/?preset=1d&from=2026-10-16%2000:00');
      expect(store.timePreset()).toBe('24h');
      expect(store.customFrom()).toBe('2026-10-16 11:42');
    });

    it('keeps a custom window exactly as written', async () => {
      await open('/?preset=custom&from=2026-10-16%2009:00&to=2026-10-16%2013:00');
      expect(store.timePreset()).toBe('custom');
      expect(store.customFrom()).toBe('2026-10-16 09:00');
      expect(store.customTo()).toBe('2026-10-16 13:00');
      expect(lastSearch().to).toBe(new Date(2026, 9, 16, 13, 0).toISOString());
    });

    it('opens on the last 24 hours when the link says nothing', async () => {
      await open('/');
      expect(store.timePreset()).toBe('24h');
      expect(store.customFrom()).toBe('2026-10-16 11:42');
    });
  });
});

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideZonelessChangeDetection, signal } from '@angular/core';
import { Router, provideRouter } from '@angular/router';
import { LUCIDE_ICONS, LucideIconProvider, icons } from 'lucide-angular';
import { NEVER, of } from 'rxjs';
import { describe, it, expect, beforeEach, afterEach, vi, type Mock } from 'vitest';

import { EventsStore } from './store/events.store';
import { ApiService } from '../../core/services/api.service';
import { OverlayPanelRef } from '../../shared/services/overlay';
import { DateDropdownController, DateRangeDropdownComponent } from './dropdowns/date-range-dropdown';
import {
  TimePreset, customRangeLabel, endOfMinuteIso, fmtSpan, parsePreset, presetRange,
} from './store/events-filter.util';

/**
 * The Logs time range: a button in the filter bar opening a popover of quick ranges and From /
 * To date and time fields.
 *
 * <p>Only `Date` is faked. Zoneless Angular schedules change detection on setTimeout, so faking
 * timers wholesale is a trap this repo already avoids (see events.live-batching.spec.ts).</p>
 */

/** 2026-10-17 11:42:30 local — a Saturday, mid-morning, so no window here crosses midnight. */
const NOW = new Date(2026, 9, 17, 11, 42, 30);
const at = (h: number, m: number, day = 17) => new Date(2026, 9, day, h, m);

describe('time range presets and labels', () => {
  it('reads replaced presets as their successors and refuses names it does not know', () => {
    expect(parsePreset('1d')).toBe('1d');
    expect(parsePreset('24h')).toBe('1d');
    expect(parsePreset('30d')).toBeNull();
    expect(parsePreset('custom')).toBe('custom');
    expect(parsePreset('15m')).toBeNull();
    expect(parsePreset('toString')).toBeNull();
    expect(parsePreset(null)).toBeNull();
  });

  it('resolves each preset against the clock, N days back to the minute', () => {
    expect(presetRange('1d', NOW)).toEqual({ from: '2026-10-16 11:42', to: '' });
    expect(presetRange('3d', NOW)).toEqual({ from: '2026-10-14 11:42', to: '' });
    expect(presetRange('7d', NOW)).toEqual({ from: '2026-10-10 11:42', to: '' });
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

describe('Events store — the applied window', () => {
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

  it('resolves a preset on every load, not once when it was picked', () => {
    store.setTimePreset('1d');
    expect(store.customFrom()).toBe('2026-10-16 11:42');
    expect(store.rangeLabel()).toEqual({ text: 'Last 1 day', extra: '' });

    vi.setSystemTime(new Date(NOW.getTime() + 20 * 60_000));
    store.search();

    expect(store.customFrom()).toBe('2026-10-16 12:02');
    expect(lastSearch().from).toBe(at(12, 2, 16).toISOString());
    expect(lastSearch().to).toBeUndefined();
  });

  it('applies an explicit window as a custom one and searches it', () => {
    store.applyRange('2026-10-17 09:00', '2026-10-17 13:00');
    expect(store.timePreset()).toBe('custom');
    expect(lastSearch()).toEqual({ from: at(9, 0).toISOString(), to: endOfMinuteIso(at(13, 0)) });
    expect(store.rangeLabel()).toEqual({ text: '17 Oct 09:00 → 13:00', extra: '4 h' });

    store.applyRange('2026-10-17 09:00', '');
    expect(lastSearch().to).toBeUndefined();
    expect(store.rangeLabel().text).toBe('Since 17 Oct 09:00');
  });

  it('sends a To of 23:59 as everything before the next midnight, and nothing at it', () => {
    store.applyRange('2026-10-17 00:00', '2026-10-18 23:59');
    const to = lastSearch().to!;
    const midnight = at(0, 0, 19);
    // The last 100 ns tick of the minute: the server's `to` is inclusive.
    expect(to).toMatch(/:59\.9999999Z$/);
    expect(to.slice(0, 23)).toBe(new Date(midnight.getTime() - 1).toISOString().slice(0, 23));
    expect(to < midnight.toISOString()).toBe(true);
  });

  it('refuses a window that ends before it starts, or does not parse', () => {
    store.setTimePreset('3d');
    const before = searches.length;
    store.applyRange('2026-10-17 13:00', '2026-10-17 09:00');
    store.applyRange('2026-10-1', '');
    expect(searches.length).toBe(before);
    expect(store.timePreset()).toBe('3d');
  });

  it('picking a window while the tail runs stops the tail and searches the window', () => {
    store.setTimePreset('1d');
    store.toggleLive();
    expect(store.live()).toBe(true);
    const before = searches.length;

    store.setTimePreset('7d');

    expect(store.live()).toBe(false);
    expect(searches.length).toBe(before + 1);
    expect(lastSearch().from).toBe(at(11, 42, 10).toISOString());
  });

  describe('opening from a link', () => {
    const open = async (url: string) => {
      await TestBed.inject(Router).navigateByUrl(url);
      store.initFromUrl();
    };

    it('opens a preset from now, whatever start the link was made with', async () => {
      await open('/?preset=7d&from=2026-10-01%2009:00');
      expect(store.timePreset()).toBe('7d');
      expect(store.customFrom()).toBe('2026-10-10 11:42');
    });

    it('reads the replaced 24h preset as the last day', async () => {
      await open('/?preset=24h&from=2026-10-16%2000:00');
      expect(store.timePreset()).toBe('1d');
      expect(store.customFrom()).toBe('2026-10-16 11:42');
    });

    it('keeps the bounds of a link whose preset is gone', async () => {
      await open('/?preset=30m&from=2026-10-17%2011:00');
      expect(store.timePreset()).toBe('custom');
      expect(store.customFrom()).toBe('2026-10-17 11:00');
    });

    it('keeps a custom window exactly as written', async () => {
      await open('/?preset=custom&from=2026-10-16%2009:00&to=2026-10-16%2013:00');
      expect(store.timePreset()).toBe('custom');
      expect(lastSearch()).toEqual({
        from: at(9, 0, 16).toISOString(),
        to: endOfMinuteIso(at(13, 0, 16)),
      });
    });

    it('opens on the last day when the link says nothing', async () => {
      await open('/');
      expect(store.timePreset()).toBe('1d');
      expect(store.customFrom()).toBe('2026-10-16 11:42');
    });
  });
});

describe('Time range popover', () => {
  let ctrl: DateDropdownController;
  let close: Mock<() => void>;
  let fixture: ComponentFixture<DateRangeDropdownComponent>;

  const create = (preset: TimePreset, from: string, to = '') => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(NOW);
    close = vi.fn<() => void>();
    ctrl = {
      presets: [{ label: 'Last 1 day', value: '1d' }, { label: 'Last 3 days', value: '3d' }],
      timePreset: signal(preset),
      customFrom: signal(from),
      customTo: signal(to),
      setPreset: vi.fn<(p: TimePreset) => void>(),
      applyRange: vi.fn<(from: string, to: string) => void>(),
    };
    TestBed.configureTestingModule({
      providers: [
        provideZonelessChangeDetection(),
        { provide: LUCIDE_ICONS, multi: true, useValue: new LucideIconProvider(icons) },
        { provide: OverlayPanelRef, useValue: { data: ctrl, close } },
      ],
    });
    fixture = TestBed.createComponent(DateRangeDropdownComponent);
    fixture.detectChanges();
    return fixture.componentInstance;
  };

  const el = (selector: string) => fixture.nativeElement.querySelector(selector) as HTMLElement;

  afterEach(() => {
    TestBed.resetTestingModule();
    vi.useRealTimers();
  });

  it('starts from the applied window and says what it covers', () => {
    const p = create('custom', '2026-10-17 09:00');
    expect(p.fromDate()).toBe('2026-10-17');
    expect(p.fromTime()).toBe('09:00');
    expect(p.toDate()).toBe('');
    expect(p.summary()).toBe('From 17 Oct 09:00 → now');
  });

  it('a quick range applies at once and closes', () => {
    const p = create('1d', '2026-10-16 11:42');
    p.pickPreset('3d');
    expect(ctrl.setPreset).toHaveBeenCalledWith('3d');
    expect(close).toHaveBeenCalled();
  });

  it('nothing reaches the page until Apply', () => {
    const p = create('custom', '2026-10-17 09:00');
    p.setTime('from', '10:30');
    expect(ctrl.applyRange).not.toHaveBeenCalled();
    p.apply();
    expect(ctrl.applyRange).toHaveBeenCalledWith('2026-10-17 10:30', '');
  });

  it('shows no time until asked; a day alone runs from 00:00 to 23:59', () => {
    const p = create('1d', '2026-10-16 11:42');
    expect(p.timesShown()).toBe(false);
    expect(p.summary()).toBe('From 16 Oct 11:42 → now');

    p.setDate('to', '2026-10-17');
    expect(p.summary()).toBe('From 16 Oct → 17 Oct');
    p.apply();
    expect(ctrl.applyRange).toHaveBeenCalledWith('2026-10-16 00:00', '2026-10-17 23:59');
  });

  it('Set times shows both, From 00:00 and To 23:59, and gives an open To today', () => {
    const p = create('1d', '2026-10-16 11:42');
    p.setTimes();
    expect(p.timesShown()).toBe(true);
    expect(p.fromValue()).toBe('2026-10-16 00:00');
    expect(p.toDate()).toBe('2026-10-17');
    expect(p.toValue()).toBe('2026-10-17 23:59');
    expect(p.summary()).toBe('From 16 Oct 00:00 → 17 Oct 23:59');

    p.pickHour('from', '09');
    expect(p.fromValue()).toBe('2026-10-16 09:00');

    p.wholeDays();
    expect(p.timesShown()).toBe(false);
    expect(p.fromValue()).toBe('2026-10-16 00:00');
  });

  it('reopening a window keeps its times as applied, 00:00 and 23:59 included', () => {
    const p = create('custom', '2026-10-16 10:00', '2026-10-17 23:59');
    expect(p.timesShown()).toBe(true);
    expect(p.fromTime()).toBe('10:00');
    expect(p.toTime()).toBe('23:59');
    expect(p.toValue()).toBe('2026-10-17 23:59');
  });

  it('Set times keeps times already set; Whole days only hides them', () => {
    const p = create('custom', '2026-10-16 00:00', '2026-10-17 23:59');
    expect(p.timesShown()).toBe(false);
    p.setTimes();
    expect(p.fromTime()).toBe('00:00');
    p.setTime('from', '10:00');

    p.wholeDays();
    expect(p.fromValue()).toBe('2026-10-16 00:00');
    p.setTimes();
    expect(p.fromTime()).toBe('10:00');
    expect(p.fromValue()).toBe('2026-10-16 10:00');
  });

  it('a press outside the open picker closes it, a press inside does not', () => {
    const p = create('custom', '2026-10-17 09:00');
    p.toggle('from', 'time');
    fixture.detectChanges();

    el('.trp-tp-head').dispatchEvent(new Event('pointerdown', { bubbles: true }));
    expect(p.isOpen('from', 'time')).toBe(true);

    el('.trp-title').dispatchEvent(new Event('pointerdown', { bubbles: true }));
    expect(p.picker()).toBeNull();
  });

  it('Esc closes the open picker and goes no further', () => {
    const p = create('custom', '2026-10-17 09:00');
    p.toggle('from', 'time');
    fixture.detectChanges();
    const reachedDocument = vi.fn();
    document.addEventListener('keydown', reachedDocument);

    el('.trp-tp-item').dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));

    document.removeEventListener('keydown', reachedDocument);
    expect(p.picker()).toBeNull();
    expect(reachedDocument).not.toHaveBeenCalled();
    expect(close).not.toHaveBeenCalled();
  });

  it('picking the minute closes the time picker; the hour leaves it open', () => {
    const p = create('custom', '2026-10-17 09:00');
    p.toggle('from', 'time');
    p.pickHour('from', '11');
    expect(p.isOpen('from', 'time')).toBe(true);
    p.pickMinute('from', '30');
    expect(p.picker()).toBeNull();
    expect(p.fromTime()).toBe('11:30');
  });

  it('a To time with no To day is on the From day', () => {
    const p = create('custom', '2026-10-17 09:00');
    p.setTime('to', '13:00');
    expect(p.summary()).toBe('From 17 Oct 09:00 → 17 Oct 13:00');
    p.apply();
    expect(ctrl.applyRange).toHaveBeenCalledWith('2026-10-17 09:00', '2026-10-17 13:00');
  });

  it('keeps Apply off for a To before From, a day that does not exist, or a bad time', () => {
    const p = create('custom', '2026-10-17 09:00');
    p.setDate('to', '2026-10-16');
    expect(p.canApply()).toBe(false);
    expect(p.dateInvalid('to')).toBe(true);

    p.setDate('to', '2026-02-31');
    expect(p.canApply()).toBe(false);

    p.setDate('to', '');
    p.setTime('from', '25:00');
    expect(p.canApply()).toBe(false);
    expect(p.timeInvalid('from')).toBe(true);
  });

  it('Apply on untouched fields re-runs the preset instead of freezing it', () => {
    const p = create('7d', '2026-10-10 11:42');
    p.apply();
    expect(ctrl.setPreset).toHaveBeenCalledWith('7d');
    expect(ctrl.applyRange).not.toHaveBeenCalled();
  });

  it('the time picker sets hours and minutes, and Now on To leaves it open', () => {
    const p = create('custom', '2026-10-17 09:00', '2026-10-17 13:00');
    p.toggle('from', 'time');
    p.pickHour('from', '08');
    p.pickMinute('from', '45');
    expect(p.fromTime()).toBe('08:45');

    p.pickNow('to');
    expect(p.toDate()).toBe('');
    expect(p.toTime()).toBe('');
  });

  it('the calendar sets the day, and Whole days runs 00:00 to 23:59', () => {
    const p = create('custom', '2026-10-17 09:00');
    p.toggle('to', 'date');
    expect(p.calLabel()).toBe('October 2026');
    p.pickDay('to', '2026-10-18');
    expect(p.picker()).toBeNull();
    p.wholeDays();
    p.apply();
    expect(ctrl.applyRange).toHaveBeenCalledWith('2026-10-17 00:00', '2026-10-18 23:59');
  });

  it('Reset goes back to the last day', () => {
    const p = create('custom', '2026-10-17 09:00');
    p.reset();
    expect(ctrl.setPreset).toHaveBeenCalledWith('1d');
    expect(close).toHaveBeenCalled();
  });
});

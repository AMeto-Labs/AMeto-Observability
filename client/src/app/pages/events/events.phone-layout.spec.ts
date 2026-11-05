import { TestBed } from '@angular/core/testing';
import { provideZonelessChangeDetection, signal } from '@angular/core';
import { provideRouter } from '@angular/router';
import { NEVER, of } from 'rxjs';
import { describe, it, expect, afterEach, vi } from 'vitest';

import {
  DAY_SEPARATOR_H, EventsComponent, PHONE_ROW_ESTIMATE, isDaySeparator, withDaySeparators,
} from './events';
import { ApiService } from '../../core/services/api.service';
import { ViewportService } from '../../core/services/viewport.service';
import { EventDto } from '../../core/models/event.model';

/**
 * The Logs page on a phone (design canvas, "Логи на телефоне"): two-line rows estimated at 60px
 * and measured, and a 24px day separator before the first row of each day that is not today.
 * jsdom lays nothing out, so what is pinned here is what the virtualizer is TOLD — the estimates
 * and the items — and the list the separators are inserted into.
 */

/** 2026-11-05 11:42 local: the day the page is open on. */
const NOW = new Date(2026, 10, 5, 11, 42, 30);

const ev = (id: string, at: Date, service = 'api'): EventDto => ({
  id, '@t': at.toISOString(), '@mt': `event ${id}`, '@l': 'Information', '@service': service,
});

const api: Partial<ApiService> = {
  streamEvents: () => NEVER,
  streamLive: () => NEVER,
  aggregate: () => NEVER,
  getServiceNames: () => of([]),
  recordSearch: () => of(undefined as void),
};

/** The ViewportService as a phone, a tablet or a desktop with a mouse. */
const viewport = (phone: boolean, tablet = false) => ({
  phone: signal(phone), tablet: signal(tablet), coarse: signal(phone),
});

const render = async (vp: ReturnType<typeof viewport>, rows: EventDto[]) => {
  TestBed.configureTestingModule({
    providers: [
      provideZonelessChangeDetection(),
      provideRouter([]),
      { provide: ApiService, useValue: { ...api, streamEvents: () => of(...rows) } },
      { provide: ViewportService, useValue: vp },
    ],
  });
  TestBed.overrideComponent(EventsComponent, {
    set: { template: '<div #eventsScroll style="height: 300px; overflow: auto"></div>', imports: [] },
  });
  const fixture = TestBed.createComponent(EventsComponent);
  await fixture.whenStable();
  return fixture.componentInstance;
};

afterEach(() => {
  TestBed.resetTestingModule();
  vi.useRealTimers();
});

describe('withDaySeparators', () => {
  const today = '2026-11-05';

  it('puts a separator before the first row of each new day, none before today', () => {
    const rows = [
      ev('a', new Date(2026, 10, 5, 9, 0)),
      ev('b', new Date(2026, 10, 5, 8, 0)),
      ev('c', new Date(2026, 10, 4, 23, 59)),
      ev('d', new Date(2026, 10, 4, 1, 0)),
      ev('e', new Date(2026, 10, 2, 12, 0)),
    ];
    const items = withDaySeparators(rows, today);
    expect(items.map(i => isDaySeparator(i) ? `[${i.date}]` : i.id))
      .toEqual(['a', 'b', '[2026-11-04]', 'c', 'd', '[2026-11-02]', 'e']);
    expect(items.filter(isDaySeparator).map(i => i.id)).toEqual(['day:2026-11-04', 'day:2026-11-02']);
  });

  it('heads a list whose first row is not from today', () => {
    const items = withDaySeparators([ev('x', new Date(2026, 10, 3, 10, 0))], today);
    expect(items.map(i => isDaySeparator(i) ? `[${i.date}]` : i.id)).toEqual(['[2026-11-03]', 'x']);
  });

  it('is the rows themselves for an empty list', () => {
    expect(withDaySeparators([], today)).toEqual([]);
  });
});

describe('Events page on a phone — the virtual list', () => {
  it('estimates two-line rows at 60px and day separators at 24, and measures the rows', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(NOW);
    const page = await render(viewport(true), [
      ev('a', new Date(2026, 10, 5, 9, 0)),
      ev('c', new Date(2026, 10, 4, 23, 59)),
    ]);
    const items = page.listItems();
    expect(items.map(i => isDaySeparator(i) ? 'day' : i.id)).toEqual(['a', 'day', 'c']);

    const opts = page.virtualizer.options();
    expect(opts.count).toBe(3);
    expect(opts.estimateSize(0)).toBe(PHONE_ROW_ESTIMATE);
    expect(opts.estimateSize(1)).toBe(DAY_SEPARATOR_H);
    expect(opts.estimateSize(2)).toBe(PHONE_ROW_ESTIMATE);
    expect(opts.getItemKey(1)).toBe('day:2026-11-04');
    expect(page.measured()).toBe(true);
  });

  it('on a desktop the list is the rows, 24px each and unmeasured', async () => {
    const page = await render(viewport(false), [
      ev('a', new Date(2026, 10, 5, 9, 0)),
      ev('c', new Date(2026, 10, 4, 23, 59)),
    ]);
    expect(page.listItems()).toBe(page.store.displayedEvents());
    const opts = page.virtualizer.options();
    expect(opts.count).toBe(2);
    expect(opts.estimateSize(0)).toBe(24);
    // The library fills a default measurer into its options; what the page decides is here.
    expect(page.measured()).toBe(false);
  });

  it('"Older logs" goes to the last item, separators counted', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(NOW);
    const page = await render(viewport(true), [
      ev('a', new Date(2026, 10, 5, 9, 0)),
      ev('c', new Date(2026, 10, 4, 23, 59)),
    ]);
    const scrollToIndex = vi.spyOn(page.virtualizer, 'scrollToIndex');
    page.scrollToOlderLogs();
    expect(scrollToIndex).toHaveBeenCalledWith(2, { behavior: 'auto' });
  });

  it('the tablet keeps the table: 120px service column, no measuring, the time alone', async () => {
    const page = await render(viewport(false, true), [ev('a', new Date(2026, 10, 5, 9, 0), 'a-very-long-service-name')]);
    expect(page.svcCol()).toBe('120px');
    expect(page.drawerWidth()).toBeNull();
    expect(page.measured()).toBe(false);
  });
});

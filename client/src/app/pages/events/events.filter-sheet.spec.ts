import { TestBed } from '@angular/core/testing';
import { provideZonelessChangeDetection } from '@angular/core';
import { provideRouter } from '@angular/router';
import { LUCIDE_ICONS, LucideIconProvider, icons } from 'lucide-angular';
import { NEVER, of } from 'rxjs';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

import { EventsStore } from './store/events.store';
import { ApiService } from '../../core/services/api.service';
import { EventQueryParams } from '../../core/models/event.model';
import { excludeLikeFilter, findLikeFilter, quoted } from './store/events-filter.util';
import {
  EventsFilterSheetComponent, fromInputDateTime, toInputDateTime,
} from './components/events-filter-sheet/events-filter-sheet';

/**
 * The phone's filter sheet applies its whole draft in ONE load (design canvas, "Телефон — лист
 * фильтров логов"), where the pickers it replaces would have loaded once each; its "Filters · N"
 * counts the three restrictions it holds. jsdom runs no layout, so the store is exercised alone.
 */
describe('Events store — the filter sheet', () => {
  let store: InstanceType<typeof EventsStore>;
  let searches: EventQueryParams[];

  beforeEach(() => {
    searches = [];
    const api: Partial<ApiService> = {
      streamEvents: ((q: EventQueryParams) => { searches.push(q); return NEVER; }) as ApiService['streamEvents'],
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
        { provide: LUCIDE_ICONS, multi: true, useValue: new LucideIconProvider({ ...icons }) },
        EventsStore,
      ],
    });
    store = TestBed.inject(EventsStore);
  });

  afterEach(() => {
    TestBed.resetTestingModule();
    localStorage.clear();
    vi.useRealTimers();
  });

  it('applies levels, services, the preset, the page size and wrap in one load, and closes', () => {
    store.openFilterSheet();
    expect(store.filterSheetOpen()).toBe(true);

    store.applyFilterSheet({
      preset: '7d', from: '', to: '',
      levels: new Set(['Warning', 'Error']),
      services: new Set(['payments']),
      pageSize: 150,
      wrap: true,
    });

    expect(searches).toHaveLength(1);
    expect(store.filter()).toBe("@l in ['Warning', 'Error'] and @service = 'payments'");
    expect(store.filterInput()).toBe(store.filter());
    expect(store.timePreset()).toBe('7d');
    expect(store.pageSize()).toBe(150);
    expect(store.wrapMessages()).toBe(true);
    expect(localStorage.getItem('Ameto-events-wrap')).toBe('1');
    expect(store.filterSheetOpen()).toBe(false);
    expect(searches[0].count).toBe(150);
    expect(searches[0].levels).toBe('Warning,Error');
  });

  it('a custom window is applied as the range popover applies it, and a bad one is refused', () => {
    store.applyFilterSheet({
      preset: 'custom', from: '2026-11-02 14:22', to: '2026-11-03 09:00',
      levels: new Set(['Verbose', 'Debug', 'Information', 'Warning', 'Error', 'Fatal']),
      services: new Set(), pageSize: 50, wrap: false,
    });
    expect(store.timePreset()).toBe('custom');
    expect(store.customFrom()).toBe('2026-11-02 14:22');
    expect(store.customTo()).toBe('2026-11-03 09:00');
    expect(store.filter()).toBe('');

    // An end before the start: the window stays where it was.
    store.applyFilterSheet({
      preset: 'custom', from: '2026-11-03 14:22', to: '2026-11-02 09:00',
      levels: new Set(['Verbose', 'Debug', 'Information', 'Warning', 'Error', 'Fatal']),
      services: new Set(), pageSize: 50, wrap: false,
    });
    expect(store.customFrom()).toBe('2026-11-02 14:22');
    expect(store.customTo()).toBe('2026-11-03 09:00');
  });

  it('stops a running tail only when the window moved', () => {
    store.startLive();
    expect(store.live()).toBe(true);
    const all = new Set(['Verbose', 'Debug', 'Information', 'Warning', 'Error', 'Fatal']);

    store.applyFilterSheet({ preset: '1d', from: '', to: '', levels: all, services: new Set(), pageSize: 100, wrap: false });
    expect(store.live()).toBe(true);

    store.applyFilterSheet({ preset: '3d', from: '', to: '', levels: all, services: new Set(), pageSize: 100, wrap: false });
    expect(store.live()).toBe(false);
    expect(store.timePreset()).toBe('3d');

    // A custom window: moving to it is a move; re-applying it as it stands, to change the levels
    // under it, is not; other bounds are.
    store.applyFilterSheet({ preset: 'custom', from: '2026-11-02 14:22', to: '', levels: all, services: new Set(), pageSize: 100, wrap: false });
    store.startLive();
    expect(store.live()).toBe(true);
    store.applyFilterSheet({ preset: 'custom', from: '2026-11-02 14:22', to: '', levels: new Set(['Error']), services: new Set(), pageSize: 100, wrap: false });
    expect(store.live()).toBe(true);
    expect(store.filter()).toBe("@l = 'Error'");
    store.applyFilterSheet({ preset: 'custom', from: '2026-11-02 14:22', to: '2026-11-03 09:00', levels: new Set(['Error']), services: new Set(), pageSize: 100, wrap: false });
    expect(store.live()).toBe(false);
    expect(store.customTo()).toBe('2026-11-03 09:00');
  });

  it('under an aggregation applies the window alone: there is no clause to splice', () => {
    store.applyFilter('select count(*) from stream group by @service');
    expect(store.isAggregation()).toBe(true);
    store.applyFilterSheet({
      preset: '3d', from: '', to: '',
      levels: new Set(['Error']), services: new Set(['api']), pageSize: 50, wrap: false,
    });
    expect(store.filter()).toBe('select count(*) from stream group by @service');
    expect(store.timePreset()).toBe('3d');
    expect(store.filterSheetOpen()).toBe(false);
  });

  it('under an aggregation the sheet offers the time range alone, with the chip strip\'s note', async () => {
    store.applyFilter('select count(*) from stream group by @service');
    const fixture = TestBed.createComponent(EventsFilterSheetComponent);
    await fixture.whenStable();
    const el = fixture.nativeElement as HTMLElement;
    expect(el.querySelector('.fs-presets')).not.toBeNull();
    expect(el.querySelector('.fs-range')).not.toBeNull();
    expect(el.querySelector('.fs-svc-search')).toBeNull();
    expect(el.querySelector('.bs-seg')).toBeNull();
    expect(el.querySelector('.bs-switch')).toBeNull();
    expect(el.querySelector('.fs-agg-note')?.textContent).toContain('Aggregation');
    expect(el.querySelector('.bs-note')?.textContent).toContain('Aggregation');

    // Off the aggregation, the sections are back.
    store.applyFilter('');
    await fixture.whenStable();
    expect(el.querySelector('.fs-svc-search')).not.toBeNull();
    expect(el.querySelector('.fs-agg-note')).toBeNull();
  });

  it('counts the restrictions the "Filters · N" button shows', () => {
    expect(store.restrictionCount()).toBe(0);
    store.setTimePreset('3d');
    expect(store.restrictionCount()).toBe(1);
    store.setLevels(new Set(['Error']));
    expect(store.restrictionCount()).toBe(2);
    store.setServices(new Set(['api']));
    expect(store.restrictionCount()).toBe(3);
    store.resetAll();
    expect(store.restrictionCount()).toBe(0);
  });
});

describe('the message action sheet — its expressions', () => {
  it('finds all like this on the template, quoted as the lexer reads it', () => {
    expect(findLikeFilter("User {Name} said 'hi'")).toBe("@mt = 'User {Name} said ''hi'''");
    // The lexer takes a backslash as an escape, so each one is doubled — and a template ending
    // in one no longer escapes the closing quote.
    expect(findLikeFilter('Reading C:\\data\\{File}')).toBe("@mt = 'Reading C:\\\\data\\\\{File}'");
    expect(quoted('ends with \\')).toBe("'ends with \\\\'");
    expect(findLikeFilter(undefined)).toBe("@mt = ''");
  });

  it('excludes these from the current filter, or alone when there is none', () => {
    expect(excludeLikeFilter('Tick {N}', '')).toBe("@mt <> 'Tick {N}'");
    expect(excludeLikeFilter('Tick {N}', "@l = 'Error'")).toBe("(@l = 'Error') and @mt <> 'Tick {N}'");
  });
});

describe('the filter sheet — the datetime-local fields', () => {
  it('round-trips the store\'s minute-precision bounds', () => {
    expect(toInputDateTime('2026-11-02 14:22')).toBe('2026-11-02T14:22');
    expect(fromInputDateTime('2026-11-02T14:22')).toBe('2026-11-02 14:22');
    expect(toInputDateTime('')).toBe('');
    expect(fromInputDateTime('')).toBe('');
  });
});

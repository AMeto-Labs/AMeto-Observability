import { TestBed } from '@angular/core/testing';
import { provideZonelessChangeDetection } from '@angular/core';
import { provideRouter } from '@angular/router';
import { NEVER, of } from 'rxjs';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';

import { EventsStore } from './store/events.store';
import { ApiService } from '../../core/services/api.service';
import { EventDto } from '../../core/models/event.model';

/**
 * The service clause is the SERVER's to answer. Once a filter is applied, the rows on screen are
 * what the server returned for it — and the server answers `@service` case-insensitively, under a
 * `not`, inside an `or`. The page used to narrow those rows a second time, client-side, by the
 * services it parsed out of the filter text, matching case exactly and reading any `@service`
 * clause as a selection: `not @service = 'x'` emptied the list, `@service = 'payments'` hid the
 * stored `Payments`, and `@service = 'a' or @l = 'Fatal'` hid the Fatal rows of every other
 * service. Each of those is pinned here against rows the fake server "returned".
 */
describe('Events — the service filter never hides rows the server returned', () => {
  let store: InstanceType<typeof EventsStore>;
  let answer: EventDto[];

  const ev = (id: string, service: string, level = 'Information'): EventDto => ({
    '@t': '2026-10-01T09:00:00.0000000Z', '@mt': `event ${id}`, '@l': level, '@service': service, id,
  });

  /** The server answers `filter` with `rows`; the filter is applied the way the search box applies it. */
  const serverAnswers = (filter: string, rows: EventDto[]): void => {
    answer = rows;
    store.applyFilter(filter);
  };

  const shown = (): string[] => store.displayedEvents().map(e => e.id);

  beforeEach(() => {
    answer = [];
    const api: Partial<ApiService> = {
      streamEvents: () => of(...answer),
      // Reached on paths this spec does not travel; none is under test.
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

  afterEach(() => TestBed.resetTestingModule());

  it('a negated service clause selects nothing and hides nothing', () => {
    serverAnswers("not @service = 'checkout'", [ev('1', 'billing'), ev('2', 'gateway')]);
    expect(store.selectedServices().size).toBe(0);
    expect(shown()).toEqual(['1', '2']);
  });

  it('a service literal in another case keeps the rows the server matched case-insensitively', () => {
    serverAnswers("@service = 'payments'", [ev('1', 'Payments'), ev('2', 'PAYMENTS')]);
    expect(shown()).toEqual(['1', '2']);
  });

  it('a service clause beside a top-level or keeps the other branch', () => {
    serverAnswers("@service = 'a' or @l = 'Fatal'", [ev('1', 'a'), ev('2', 'b', 'Fatal')]);
    expect(store.selectedServices().size).toBe(0);
    expect(shown()).toEqual(['1', '2']);
  });

  it('an unapplied draft previews its selection, case-insensitively', () => {
    serverAnswers('', [ev('1', 'Payments'), ev('2', 'billing')]);
    store.setFilterInput("@service = 'payments'");
    expect(shown()).toEqual(['1']);
  });

  it('the services picker still selects, and replaces its own clause', () => {
    serverAnswers("@l = 'Error'", [ev('1', 'a', 'Error')]);
    store.setServices(new Set(['a']));
    expect(store.filter()).toBe("@l = 'Error' and @service = 'a'");
    expect([...store.selectedServices()]).toEqual(['a']);

    store.setServices(new Set(['a', 'b']));
    expect(store.filter()).toBe("@l = 'Error' and @service in ['a', 'b']");
    expect([...store.selectedServices()]).toEqual(['a', 'b']);

    store.setServices(new Set());
    expect(store.filter()).toBe("@l = 'Error'");
  });
});

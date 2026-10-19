import { TestBed } from '@angular/core/testing';
import { provideZonelessChangeDetection } from '@angular/core';
import { Router, provideRouter } from '@angular/router';
import { NEVER, of } from 'rxjs';
import { describe, it, expect, afterEach, vi } from 'vitest';

import { EventsComponent } from './events';
import { EventsStore } from './store/events.store';
import { ApiService } from '../../core/services/api.service';

const api: Partial<ApiService> = {
  streamEvents: () => NEVER,
  streamLive: () => NEVER,
  aggregate: () => NEVER,
  getServiceNames: () => of([]),
  recordSearch: () => of(undefined as void),
};

const configure = (): void => {
  TestBed.configureTestingModule({
    providers: [
      provideZonelessChangeDetection(),
      provideRouter([]),
      { provide: ApiService, useValue: api },
    ],
  });
};

afterEach(() => {
  TestBed.resetTestingModule();
  localStorage.clear();
});

/**
 * Turning wrap off must bring the list back to one-line rows.
 *
 * <p>The virtualizer keeps every MEASURED row height by row key and prefers it to the estimate
 * whatever `measureElement` says, so after wrap → off every row that had been measured kept its
 * wrapped height and the list stayed spread out. The page now drops the measured heights on
 * every switch. jsdom lays nothing out, so what is pinned here is that drop; the page's own
 * template is swapped for the scroll element alone, which is all the virtualizer needs.</p>
 */
describe('Events page — switching wrap starts the row heights over', () => {
  it('drops the measured heights when wrap goes on and when it goes off again', async () => {
    configure();
    TestBed.overrideComponent(EventsComponent, {
      set: { template: '<div #eventsScroll style="height: 300px; overflow: auto"></div>', imports: [] },
    });
    const fixture = TestBed.createComponent(EventsComponent);
    await fixture.whenStable();
    const page = fixture.componentInstance;
    const measure = vi.spyOn(page.virtualizer, 'measure');

    page.store.toggleWrap();
    await fixture.whenStable();
    expect(page.store.wrapMessages()).toBe(true);
    expect(measure).toHaveBeenCalledTimes(1);

    page.store.toggleWrap();
    await fixture.whenStable();
    expect(page.store.wrapMessages()).toBe(false);
    expect(measure).toHaveBeenCalledTimes(2);
  });
});

/**
 * The SERVICE column is as wide as the longest service name on the page, between its header's
 * width and 155px, and one width for every row. jsdom has no canvas, so names are measured by
 * the page's per-character fallback (7px a character); what is pinned is the bounds.
 */
describe('Events page — the SERVICE column fits its longest name, up to 155px', () => {
  const render = async (services: string[]) => {
    TestBed.configureTestingModule({
      providers: [
        provideZonelessChangeDetection(),
        provideRouter([]),
        {
          provide: ApiService,
          useValue: {
            ...api,
            streamEvents: () => of(...services.map((s, i) => ({
              id: `e${i}`, '@t': new Date(2026, 9, 19, 9, 0, i).toISOString(), '@mt': 'm', '@l': 'Information', '@service': s,
            }))),
          } as Partial<ApiService>,
        },
      ],
    });
    TestBed.overrideComponent(EventsComponent, {
      set: { template: '<div #eventsScroll></div>', imports: [] },
    });
    const fixture = TestBed.createComponent(EventsComponent);
    await fixture.whenStable();
    return fixture.componentInstance;
  };

  it('is no narrower than its header for short names', async () => {
    const page = await render(['api', 'db']);
    expect(page.svcColWidth()).toBe(70);
  });

  it('follows the longest name between the bounds', async () => {
    const page = await render(['api', 'payments-gw']);          // 11 × 7 + 27 = 104
    expect(page.svcColWidth()).toBe(104);
  });

  it('stops at 155px, however long a name is', async () => {
    const page = await render(['WalletOperator.API.Controllers.Dealer']);
    expect(page.svcColWidth()).toBe(155);
  });
});

/** Wrap is kept in the link (`wrap=1`) and in localStorage; the link wins. */
describe('Events store — wrap is remembered', () => {
  const open = async (url: string) => {
    configure();
    TestBed.configureTestingModule({ providers: [EventsStore] });
    const router = TestBed.inject(Router);
    await router.navigateByUrl(url);
    const store = TestBed.inject(EventsStore);
    store.initFromUrl();
    return { store, router };
  };

  it('opens wrapped from a link that says so', async () => {
    const { store } = await open('/?wrap=1');
    expect(store.wrapMessages()).toBe(true);
  });

  it('without a word in the link, opens the way it was left', async () => {
    localStorage.setItem('Ameto-events-wrap', '1');
    const { store } = await open('/');
    expect(store.wrapMessages()).toBe(true);
  });

  it('the link wins over what was left', async () => {
    localStorage.setItem('Ameto-events-wrap', '1');
    const { store } = await open('/?wrap=0');
    expect(store.wrapMessages()).toBe(false);
  });

  it('a toggle is written to both', async () => {
    const { store, router } = await open('/');
    store.toggleWrap();
    expect(localStorage.getItem('Ameto-events-wrap')).toBe('1');
    await vi.waitFor(() => expect(router.url).toContain('wrap=1'));

    store.toggleWrap();
    expect(localStorage.getItem('Ameto-events-wrap')).toBe('0');
    await vi.waitFor(() => expect(router.url).not.toContain('wrap='));
  });
});

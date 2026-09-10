import { TestBed } from '@angular/core/testing';
import { provideZonelessChangeDetection } from '@angular/core';
import { provideRouter } from '@angular/router';
import { Observable, Subject, NEVER, of } from 'rxjs';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

import { EventsStore } from './store/events.store';
import { ApiService } from '../../core/services/api.service';
import { EventDto } from '../../core/models/event.model';

/**
 * The live tail publishes ONCE PER FRAME, and the frame it publishes is in the right order.
 *
 * <p>The server coalesces writes on a 100 ms floor and answers a poll with a whole page, so
 * "one event" is not the unit anything arrives in: a poll lands as a burst of hundreds of SSE
 * frames inside a couple of milliseconds. The store used to publish each one separately — a
 * state write, a fresh `events` identity (which busts the virtualizer's measurement memo and
 * rewrites the transform on every rendered row), and a change-detection pass, several hundred
 * times between two paints anyone could see. That is what "grubo" meant, and none of it was
 * pinned by a test: Traces is smooth because four specs fail the moment it stops being, and
 * Events was rough partly because nothing here could fail.</p>
 *
 * <p>The most dangerous line in the fix is a single `batch.reverse()`. The tail is a FORWARD
 * query, so it streams OLDEST first, and the old code landed newest-on-top only as a side
 * effect of prepending one event at a time. Batching without reversing produces timestamps
 * that run the wrong way INSIDE each ~100 ms chunk and correctly between chunks — a wrongness
 * that reads as a rendering glitch rather than a bug. Claims 2 and 5 are that line and the
 * teardown it depends on; the rest is the cadence itself.</p>
 */

/** Ascending in both `@t` and `id` — the shape the wire actually has. */
function makeEvent(n: number): EventDto {
  return {
    '@t': new Date(Date.UTC(2026, 0, 1, 0, 0, 0, 0) + n * 1000).toISOString(),
    '@mt': `event ${n}`,
    '@l': 'Information',
    'service.name': 'svc',
    id: `e${String(n).padStart(5, '0')}`,
  } as EventDto;
}

describe('Events live tail — one publication per frame, newest first', () => {
  let store: InstanceType<typeof EventsStore>;
  let tail: Subject<EventDto>;
  let opened: number;
  let frames: FrameRequestCallback[];

  /** Runs every frame callback booked since the last call, as one browser frame would. */
  const runFrame = (): void => { frames.splice(0).forEach(cb => cb(0)); };

  beforeEach(() => {
    // The frame clock is stubbed directly rather than through vi.useFakeTimers: zoneless
    // Angular schedules change detection on setTimeout, so faking timers wholesale is a trap
    // this repo already avoids (see the traces fake-clock specs, which fake only setInterval).
    frames = [];
    vi.stubGlobal('requestAnimationFrame', (cb: FrameRequestCallback) => frames.push(cb));
    vi.stubGlobal('cancelAnimationFrame', () => { /* the splice in runFrame is the cancel */ });

    tail = new Subject<EventDto>();
    opened = 0;

    const api: Partial<ApiService> = {
      streamLive: () => new Observable<EventDto>(sub => {
        opened++;
        const s = tail.subscribe(sub);
        return () => s.unsubscribe();
      }),
      // The store reaches for these on the paths a restart travels; none is under test.
      streamEvents: () => NEVER,
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
    store.toggleLive();
  });

  afterEach(() => {
    // Destroying the injector runs the store's onDestroy hook, which is where the SSE
    // subscription, the pending frame and both live timers are released. (_disposeStreams is
    // not reachable from here: @ngrx/signals treats an underscore-prefixed member as private
    // and keeps it off the public store type — the hook is the supported way in.)
    TestBed.resetTestingModule();
    // Without this the next file in the same jsdom inherits a frame clock that never fires.
    vi.unstubAllGlobals();
  });

  it('a burst of 200 frames writes the list once, not two hundred times', () => {
    const before = store.events();
    for (let i = 0; i < 200; i++) tail.next(makeEvent(i));

    // Identity, not length: the whole cost of the old path was that every arrival minted a new
    // array and everything downstream of it recomputed.
    expect(store.events()).toBe(before);

    runFrame();
    expect(store.events()).not.toBe(before);
    expect(store.events().length).toBe(200);
  });

  it('the newest event is on top after a flush — the wire is ascending and the batch is reversed', () => {
    for (let i = 0; i < 5; i++) tail.next(makeEvent(i));
    runFrame();

    const rows = store.events();
    expect(rows[0].id).toBe('e00004');
    expect(rows[rows.length - 1].id).toBe('e00000');
    for (let i = 1; i < rows.length; i++)
      expect(rows[i - 1]['@t'] > rows[i]['@t']).toBe(true);
  });

  it('nothing is lost across a flush boundary, and the cap holds', () => {
    for (let i = 0; i < 30; i++) tail.next(makeEvent(i));
    runFrame();
    for (let i = 30; i < 60; i++) tail.next(makeEvent(i));
    runFrame();

    const rows = store.events();
    expect(rows.length).toBe(60);
    expect(rows[0].id).toBe('e00059');
    expect(rows[59].id).toBe('e00000');
    // Still one descending run across the join, which is where a batch that forgot to reverse
    // would look right within each half and wrong between them.
    for (let i = 1; i < rows.length; i++)
      expect(rows[i - 1]['@t'] > rows[i]['@t']).toBe(true);
    expect(rows.length).toBeLessThanOrEqual(store.liveBufferSize());
  });

  it('a batch bigger than a viewport flashes nothing; a single arrival flashes', () => {
    for (let i = 0; i < 200; i++) tail.next(makeEvent(i));
    runFrame();
    // A highlight on every visible row marks nothing — past a dozen rows in a frame it is a
    // full-list strobe, which is what the old one-second-per-event window degenerated into.
    expect(store.newEventIds().size).toBe(0);

    tail.next(makeEvent(1000));
    runFrame();
    expect(store.newEventIds().size).toBe(1);
  });

  it('a restart mid-burst never lands the old batch on the new list', () => {
    for (let i = 0; i < 50; i++) tail.next(makeEvent(i));
    // Routes through loadEvents → startLive, the mid-tail restart path. It was the one reset
    // in the store that emptied the highlight set without cancelling what fed it.
    store.setLevels(new Set(['Error']));
    expect(opened).toBe(2);

    runFrame();
    expect(store.events()).toEqual([]);
  });

  it('rows held while the reader is away are released newest-first, exactly once', () => {
    for (let i = 0; i < 10; i++) tail.next(makeEvent(i));
    runFrame();
    expect(store.events().length).toBe(10);

    // Scrolled away: the tail stops writing altogether rather than being anchored.
    store.setFollow(false);
    const parked = store.events();
    for (let i = 10; i < 25; i++) tail.next(makeEvent(i));
    runFrame();
    expect(store.events()).toBe(parked);
    expect(store.livePending()).toBe(15);

    store.resumeFollow();
    const rows = store.events();
    expect(rows.length).toBe(25);
    expect(rows[0].id).toBe('e00024');
    expect(store.livePending()).toBe(0);
    // `held` is ALREADY newest-first, so resumeFollow must not reverse a second time. This is
    // the other place in the store where wire order can go silently wrong.
    for (let i = 1; i < rows.length; i++)
      expect(rows[i - 1]['@t'] > rows[i]['@t']).toBe(true);
  });

  it('the drawer survives its row being evicted by the tail', () => {
    for (let i = 0; i < 5; i++) tail.next(makeEvent(i));
    runFrame();
    store.selectEvent('e00002');
    expect(store.selectedEvent()?.id).toBe('e00002');

    // Turn the buffer over completely; the selected row is no longer in `events`.
    for (let i = 100; i < 100 + store.liveBufferSize(); i++) tail.next(makeEvent(i));
    runFrame();
    expect(store.events().some(e => e.id === 'e00002')).toBe(false);
    // The panel the user is reading must not unmount because traffic they did not create
    // pushed its row out of a ring buffer.
    expect(store.selectedEvent()?.id).toBe('e00002');
  });

  it('retry after an aggregation runs the aggregation, not the tail that preceded it', () => {
    tail.next(makeEvent(0));
    runFrame();
    expect(store.live()).toBe(true);

    // An aggregation ends the tail. Retry then belongs to the table on screen — reopening the
    // stream would send `select … group by …` to an endpoint that refuses it by design, and
    // the aggregation branch of loadEvents returns before the line that used to clear this.
    store.applyFilter("select count(*) group by ['service.name']");
    expect(store.live()).toBe(false);
    const before = opened;

    store.retry();
    expect(opened).toBe(before);
  });

  it('stopping the tail freezes what is on screen instead of reloading it', () => {
    for (let i = 0; i < 10; i++) tail.next(makeEvent(i));
    runFrame();
    const frozen = store.events();

    store.toggleLive();
    expect(store.live()).toBe(false);
    expect(store.events()).toEqual(frozen);
  });

  it('ending the tail lands the held rows rather than deleting them', () => {
    for (let i = 0; i < 5; i++) tail.next(makeEvent(i));
    runFrame();
    store.setFollow(false);
    for (let i = 5; i < 20; i++) tail.next(makeEvent(i));
    runFrame();
    expect(store.livePending()).toBe(15);

    // Stop, with rows still behind the pill. They are one click from being read; ending the
    // stream must not be what deletes them.
    store.toggleLive();
    expect(store.live()).toBe(false);
    expect(store.events().length).toBe(20);
    expect(store.events()[0].id).toBe('e00019');
  });

  it('a stream that dies keeps the held rows too', () => {
    for (let i = 0; i < 5; i++) tail.next(makeEvent(i));
    runFrame();
    store.setFollow(false);
    for (let i = 5; i < 12; i++) tail.next(makeEvent(i));
    runFrame();

    tail.error(new Error('Live tail stopped'));
    expect(store.live()).toBe(false);
    expect(store.error()).toBe('Live tail stopped');
    expect(store.events().length).toBe(12);
  });

  it('a tab round-trip keeps the list, the held rows, the selection and the reading position', () => {
    for (let i = 0; i < 8; i++) tail.next(makeEvent(i));
    runFrame();
    store.selectEvent('e00003');
    store.setFollow(false);
    for (let i = 8; i < 20; i++) tail.next(makeEvent(i));
    runFrame();
    expect(store.livePending()).toBe(12);

    // Hidden, then back. This travels resumeLive, which must NOT be a fresh start: routing it
    // through startLive discarded the hold buffer, and once the pause outlasted the seed window
    // it also took the "cannot continue" branch and wiped the list and the drawer with it.
    store.pauseLive();
    expect(store.live()).toBe(true);
    store.resumeLive();

    expect(store.events().length).toBe(8);
    expect(store.livePending()).toBe(12);
    expect(store.liveFollow()).toBe(false);
    expect(store.selectedEvent()?.id).toBe('e00003');

    // And the re-opened stream re-sends its boundary row, which must not double up.
    tail.next(makeEvent(19));
    tail.next(makeEvent(20));
    runFrame();
    store.resumeFollow();
    const ids = store.events().map(e => e.id);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids[0]).toBe('e00020');
  });

  it('the arrival highlight outlives a single flush, so the wash fades instead of blinking', () => {
    tail.next(makeEvent(1));
    runFrame();
    expect(store.newEventIds().has('e00001')).toBe(true);

    // The next flush used to REPLACE the id set, so a row wore .is-new for exactly one flush
    // interval — about 100 ms at the server's coalescing floor — and a 900 ms wash was cut off
    // after a tenth of it. The window is what makes it a fade.
    tail.next(makeEvent(2));
    runFrame();
    expect(store.newEventIds().has('e00002')).toBe(true);
    expect(store.newEventIds().has('e00001')).toBe(true);
  });
});

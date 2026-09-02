import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { LUCIDE_ICONS, LucideIconProvider, icons, AlertCircle } from 'lucide-angular';
import { Observable, Subscriber, NEVER, of } from 'rxjs';
import { TracesComponent } from './traces';
import { ApiService } from '../../core/services/api.service';
import { TraceRowDto } from '../../core/models/span.model';
import { StreamEndDto, StreamFrame } from '../../core/models/stream.model';

/**
 * The real component, driven through its own poll timer and its own streams.
 *
 * The question under test is the one startStream used to answer with a single expression:
 * "may I repaint?" and "may I own the error?" are not the same question, and an EMPTY list is
 * where they disagree — a 15 s tick nobody asked for used to take the user's branch.
 */

// jsdom has no ResizeObserver; @tanstack/virtual observes the scroll element with one.
class NoopResizeObserver {
  observe(): void { /* no layout in jsdom */ }
  unobserve(): void { /* no-op */ }
  disconnect(): void { /* no-op */ }
}
(globalThis as any).ResizeObserver ??= NoopResizeObserver;

// jsdom does no layout, so every offsetHeight is 0 — and a virtualizer told its viewport is 0px
// high renders nothing, which would make "the DOM stays bounded" true for the wrong reason.
// virtual-core reads exactly this property (getRect / measureElement), so giving the scroll
// container a viewport and the rows their estimated height makes the window it computes real.
Object.defineProperty(HTMLElement.prototype, 'offsetHeight', {
  configurable: true,
  get(this: HTMLElement) { return this.classList.contains('trace-rows') ? 600 : 82; },
});

/**
 * One open stream, driven in the vocabulary the tests are written in: rows in, then an ending.
 *
 * <p>`complete()` with no argument is a server that ended the stream without saying anything
 * this client can read — a bare `data: {}`, which is what both log streams still send and what
 * every test here assumed before the `done` payload existed. It is therefore also the
 * degradation path under test: the header must fall back to counting rows against the ceiling,
 * and must not read an unexplained ending as a claim that the list is whole.</p>
 */
class LiveStream {
  constructor(private readonly sub: Subscriber<StreamFrame<TraceRowDto>>) {}
  next(row: TraceRowDto): void { this.sub.next({ kind: 'row', row }); }
  complete(end?: StreamEndDto): void {
    if (end) this.sub.next({ kind: 'end', end });
    this.sub.complete();
  }
  error(err: unknown): void { this.sub.error(err); }
}

/** Hands out a controllable stream per call and remembers the newest one. */
class StreamController {
  private subs: LiveStream[] = [];
  /** Every subscription ever opened. Never decremented, unlike `subs` — "did anything start
   *  a stream?" is a question a torn-down stream still answers yes to. */
  private opened = 0;

  make(): Observable<StreamFrame<TraceRowDto>> {
    return new Observable<StreamFrame<TraceRowDto>>(sub => {
      this.opened++;
      // One facade per subscription, kept rather than rebuilt, so `streams.live` is stable
      // enough to compare by identity ("the stream that must survive is the same object").
      const live = new LiveStream(sub);
      this.subs.push(live);
      return () => { this.subs = this.subs.filter(s => s !== live); };
    });
  }

  get live(): LiveStream {
    const s = this.subs[this.subs.length - 1];
    if (!s) throw new Error('no live stream');
    return s;
  }

  /** How many streams have been opened in total. */
  get started(): number { return this.opened; }

  /** Whether the newest stream is still subscribed (teardown removes it). */
  get anyLive(): boolean { return this.subs.length > 0; }
}

// The unit-test builder swallows test stdout and the browser bundle has no `fs`, but a thrown
// Error's message IS printed. Flip CAPTURE to surface raw snapshots; with it off the same
// snapshots are asserted instead.
const CAPTURE = false;
const measured: string[] = [];
function record(line: string): void {
  if (CAPTURE) measured.push(line);
}
afterAll(() => {
  if (CAPTURE && measured.length) throw new Error('MEASURED\n' + measured.join('\n'));
});

function row(id: string): TraceRowDto {
  return {
    traceId: id, spanId: 's' + id, name: 'GET /x', serviceName: 'api', services: ['api'],
    status: 'Ok', httpMethod: 'GET', httpPath: '/x', httpStatusCode: 200,
    startTimeUnixNano: 1_700_000_000_000_000_000, durationNanos: 1_000_000, spanCount: 1,
  };
}

/** Everything the two scenarios are compared on, read off the component and the rendered DOM. */
interface Snapshot {
  publishing: boolean;
  banner: string;
  listHeld: boolean;
  bgFailures: number;
  /** Milliseconds the background list refresh is quarantined for, as of right now. */
  bgRetryInMs: number;
  rows: number;
  headerSuffix: string;
  emptyState: string;
  stopButton: boolean;
}

describe('traces list — who asked decides who owns the failure', () => {
  let streams: StreamController;

  async function boot(initialRows: TraceRowDto[]) {
    streams = new StreamController();
    const api: Partial<ApiService> = {
      streamTraceList:  () => streams.make(),
      streamTraceQuery: () => streams.make(),
      getTraceStats:    () => NEVER as any,
      getServiceNames:  () => of([]) as any,
      getSearchHistory: () => NEVER as any,
      recordSearch:     () => NEVER as any,
      getTrace:         () => NEVER as any,
      getTraceLogs:     () => NEVER as any,
    };

    TestBed.configureTestingModule({
      imports: [TracesComponent],
      providers: [
        provideRouter([]),
        { provide: ApiService, useValue: api },
        { provide: LUCIDE_ICONS, multi: true, useValue: new LucideIconProvider({ ...icons, AlertCircle }) },
      ],
    });

    const fixture = TestBed.createComponent(TracesComponent);
    fixture.detectChanges();                       // ngOnInit → loadAll → the first stream
    await fixture.whenStable();

    // The page's first load delivers a COMPLETE answer of `initialRows`.
    for (const r of initialRows) streams.live.next(r);
    streams.live.complete();
    fixture.detectChanges();
    await fixture.whenStable();

    return fixture;
  }

  function snapshot(fixture: any): Snapshot {
    const c  = fixture.componentInstance as any;
    const el = fixture.nativeElement as HTMLElement;
    return {
      publishing:   c.streamPublishing(),
      banner:       c.traceqlError(),
      listHeld:     c.listHeld(),
      bgFailures:   c.bgFailures(),
      bgRetryInMs:  Math.max(0, c.nextBgListAt - Date.now()),
      rows:         c.traces().length,
      headerSuffix: (el.querySelector('.list-more-hint')?.textContent ?? '').trim(),
      emptyState:   (el.querySelector('.panel-empty')?.textContent ?? '').trim(),
      stopButton:   !!el.querySelector('.stream-stop'),
    };
  }

  /** Fires the 15 s poll through the component's own setInterval. */
  function pollTick(fixture: any): void {
    vi.advanceTimersByTime(15_000);
    fixture.detectChanges();
  }

  async function fail(fixture: any): Promise<Snapshot> {
    streams.live.error(new Error('Failed to load traces'));
    fixture.detectChanges();
    await fixture.whenStable();
    return snapshot(fixture);
  }

  beforeEach(() => {
    // setTimeout stays real, so Angular's own scheduling still runs. `Date` is faked with the
    // interval because the background backoff is now a WALL-CLOCK deadline: with a real
    // Date.now() the clock never moves between ticks, every tick lands inside the first
    // quarantine window, and the backoff tests would pass by freezing time rather than by
    // passing it. clearInterval is faked alongside setInterval so a torn-down fixture returns
    // its fake handle to the fake clock instead of to the real one.
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval', 'Date'] });
  });
  afterEach(() => vi.useRealTimers());

  it('empty list + failing poll tick: no banner, no hold, the failure is counted', async () => {
    const fixture = await boot([]);

    pollTick(fixture);
    const inFlight = snapshot(fixture);
    record('empty, tick in flight  : ' + JSON.stringify(inFlight));

    const after = await fail(fixture);
    record('empty, tick failed     : ' + JSON.stringify(after));

    expect(inFlight.publishing).toBe(false);      // an unattended tick offers nothing to stop
    expect(inFlight.stopButton).toBe(false);
    expect(after).toEqual({
      publishing: false, banner: '', listHeld: false,
      bgFailures: 1, bgRetryInMs: 15_000, rows: 0,
      headerSuffix: '', emptyState: 'No traces found', stopButton: false,
    });
  });

  it('non-empty list + failing poll tick: unchanged, still silent', async () => {
    const fixture = await boot([row('a'), row('b')]);

    pollTick(fixture);
    const inFlight = snapshot(fixture);
    expect(inFlight.publishing).toBe(false);
    expect(inFlight.rows).toBe(2);                // the silent tick has not touched the list

    const after = await fail(fixture);
    record('non-empty, tick failed : ' + JSON.stringify(after));
    expect(after).toEqual({
      publishing: false, banner: '', listHeld: false,
      bgFailures: 1, bgRetryInMs: 15_000, rows: 2,
      headerSuffix: '', emptyState: '', stopButton: false,
    });
  });

  it('empty list + failing USER refresh: the banner is theirs to see', async () => {
    const fixture = await boot([]);

    fixture.componentInstance.loadAll();          // the header's Refresh button
    fixture.detectChanges();
    const inFlight = snapshot(fixture);
    expect(inFlight.publishing).toBe(true);       // a search the user is watching arrive
    expect(inFlight.stopButton).toBe(true);

    const after = await fail(fixture);
    record('empty, USER refresh    : ' + JSON.stringify(after));
    expect(after).toEqual({
      publishing: false, banner: 'Failed to load traces', listHeld: true,
      bgFailures: 0, bgRetryInMs: 0, rows: 0,
      headerSuffix: '· the search failed — see the message above',
      emptyState: 'The search failed — see the message above', stopButton: false,
    });
  });

  it('a tick that painted into an empty list and then died leaves the list as it found it', async () => {
    const fixture = await boot([]);

    pollTick(fixture);
    streams.live.next(row('p1'));                 // an empty list has no place to protect…
    streams.live.next(row('p2'));
    fixture.detectChanges();
    // …so it paints: the first row lands immediately, the rest ride the 25-row batch.
    expect((fixture.componentInstance as any).traces().length).toBe(1);

    const after = await fail(fixture);
    record('empty, tick died mid    : ' + JSON.stringify(after));
    // …but a prefix nobody asked for must not be left behind reading as a finished answer.
    expect(after.rows).toBe(0);
    expect(after.banner).toBe('');
    expect(after.listHeld).toBe(false);
    expect(after.headerSuffix).toBe('');
    expect(after.emptyState).toBe('No traces found');
  });

  it('repeated background failures back off and then say so, without holding the list', async () => {
    const fixture = await boot([row('a')]);
    const c = fixture.componentInstance as any;

    for (let i = 1; i <= 3; i++) {
      // Walk the backoff in wall time: the quarantine is a deadline, so the way past it is to
      // let it expire. Ticks that land inside it find no stream to start; the first one after
      // it does, and there is only ever one because the rest see `streaming()`.
      const before = streams.started;
      vi.advanceTimersByTime(Math.max(0, c.nextBgListAt - Date.now()) + 15_000);
      fixture.detectChanges();
      expect(streams.started).toBe(before + 1);
      await fail(fixture);
      expect(c.bgFailures()).toBe(i);
    }
    fixture.detectChanges();
    expect(c.bgRefreshFailing()).toBe(true);      // 3 failures ≈ 45 s → the list is not live
    expect(c.listHeld()).toBe(false);             // …and is still the poll's to refresh
    expect(c.traces().length).toBe(1);
    const hint = (fixture.nativeElement as HTMLElement).querySelector('.list-stale-hint');
    expect(hint?.textContent?.trim()).toMatch(/^· live refresh failing — rows as of \d\d:\d\d:\d\d$/);
    expect((fixture.nativeElement as HTMLElement).querySelector('.list-more-hint')).toBeNull();
  });

  it('Q11: the backoff counts TIME, so alt-tabbing cannot spend it', async () => {
    const fixture = await boot([row('a')]);
    const c = fixture.componentInstance as any;

    // Four consecutive background failures — the backoff is at its 8-interval ceiling (2 min).
    for (let i = 1; i <= 4; i++) {
      vi.advanceTimersByTime(Math.max(0, c.nextBgListAt - Date.now()) + 15_000);
      fixture.detectChanges();
      await fail(fixture);
    }
    expect(c.bgFailures()).toBe(4);
    const quarantinedAt = Date.now();
    expect(c.nextBgListAt - quarantinedAt).toBe(120_000);

    // The repro: the user alt-tabs, and every return calls poll() directly. Eight of them,
    // spread over 80 s so the burst floor is not what is doing the work — the 2-minute window
    // is. Under the old tick counter each of these spent one unit of backoff (as did each
    // interval tick alongside them), and the ninth restarted the same doomed month-wide scan.
    const started = streams.started;
    for (let i = 0; i < 8; i++) {
      vi.advanceTimersByTime(10_000);
      document.dispatchEvent(new Event('visibilitychange'));
      fixture.detectChanges();
    }
    expect(Date.now() - quarantinedAt).toBe(80_000);        // still inside the window…
    expect(streams.started).toBe(started);                  // …and nothing re-ran the query
    expect(c.bgFailures()).toBe(4);                         // nothing failed again either

    // Past the deadline the retry is allowed — on the clock, not on the ninth ask.
    vi.advanceTimersByTime(45_000);
    fixture.detectChanges();
    expect(Date.now() - quarantinedAt).toBe(125_000);
    expect(streams.started).toBe(started + 1);
  });

  it('B5: re-selecting the ACTIVE Traces tab cannot abort the search that is streaming', async () => {
    const fixture = await boot([]);
    const c  = fixture.componentInstance as any;
    const el = fixture.nativeElement as HTMLElement;

    // A month-wide TraceQL search the user ran, still delivering.
    c.traceqlMode.set(true);
    c.traceqlInput = '{ .db.system = "mssql" && duration > 1s }';
    fixture.componentInstance.runTraceQLUser();
    fixture.detectChanges();
    for (let i = 0; i < 437; i++) streams.live.next(row('t' + i));
    fixture.detectChanges();
    const started = streams.started;
    expect(c.streamPublishing()).toBe(true);
    expect(el.querySelector('.stream-stop')).not.toBeNull();

    // The click: the Traces tab, already selected.
    fixture.componentInstance.setMainTab('traces');
    fixture.detectChanges();

    // The scan is not thrown away, and no replacement stream was opened behind it.
    expect(streams.anyLive).toBe(true);
    expect(streams.started).toBe(started);
    expect(c.streamPublishing()).toBe(true);                // still the user's, still stoppable
    expect(el.querySelector('.stream-stop')).not.toBeNull();
    expect((el.querySelector('.list-more-hint')?.textContent ?? '').trim()).toBe('· streaming…');

    // …and it still finishes as its own search: the rest of the rows arrive and complete.
    for (let i = 437; i < 500; i++) streams.live.next(row('t' + i));
    streams.live.complete();
    fixture.detectChanges();
    await fixture.whenStable();
    expect(c.traces().length).toBe(500);
    expect(c.streamStopped()).toBe(false);
    expect(el.querySelector('.list-more-hint')).toBeNull();  // a complete answer, said plainly
  });

  // ── The two guards above, held apart ──────────────────────────────────────────
  // The test just above passes with EITHER guard alone: the tab guard in setMainTab never
  // reaches startStream, and the origin guard in startStream refuses the call if it does. That
  // is fine for the reported bug and useless as protection — drop either one on its own and the
  // suite stays green, so the next edit to touch them gets no warning. The two tests below each
  // pin exactly one, by standing in the place the other cannot reach:
  //
  //   · the tab guard is the only thing standing when NO stream is open — the origin guard has
  //     nothing to refuse, because refusing is conditional on a stream being live;
  //   · the origin guard is the only thing standing when startStream is reached by a route that
  //     is not setMainTab at all — the tab guard is not on that path.

  it('C2: the tab guard — re-selecting the ACTIVE tab is not navigation, so it starts nothing', async () => {
    const fixture = await boot([row('a'), row('b')]);
    const c  = fixture.componentInstance as any;
    const el = fixture.nativeElement as HTMLElement;

    // The list is settled: a complete answer, no stream open. This is the state the origin
    // guard cannot cover — `streamSub` is null, so it waves the background refresh through,
    // and the ONLY thing between a click on the already-current tab and a fresh 2000-row
    // EventSource is the equality check at the top of setMainTab.
    expect(streams.anyLive).toBe(false);
    expect(c.streaming()).toBe(false);
    const started = streams.started;
    const rows    = c.traces();

    // Three clicks on the tab the user is already on.
    fixture.componentInstance.setMainTab('traces');
    fixture.componentInstance.setMainTab('traces');
    fixture.componentInstance.setMainTab('traces');
    fixture.detectChanges();

    // Nothing was asked of the server, and nothing on screen moved — not even the array
    // identity, so the virtualizer has no reason to re-diff a list that did not change.
    expect(streams.started).toBe(started);
    expect(streams.anyLive).toBe(false);
    expect(c.streaming()).toBe(false);
    expect(c.traces()).toBe(rows);
    expect(el.querySelector('.list-more-hint')).toBeNull();

    // Leaving and coming back IS navigation, and does refresh — the guard costs a re-click,
    // never a real one.
    fixture.componentInstance.setMainTab('graph');
    fixture.componentInstance.setMainTab('traces');
    fixture.detectChanges();
    expect(streams.started).toBe(started + 1);
  });

  it('C2: the origin guard — a background stream never supersedes one already delivering', async () => {
    const fixture = await boot([]);
    const c  = fixture.componentInstance as any;
    const el = fixture.nativeElement as HTMLElement;

    // A search the user is watching arrive.
    fixture.componentInstance.loadAll();
    fixture.detectChanges();
    for (let i = 0; i < 437; i++) streams.live.next(row('t' + i));
    fixture.detectChanges();
    const started   = streams.started;
    const searching = streams.live;                 // the subscriber that must survive
    const rows      = c.traces().length;            // the flushed prefix, tail still buffered
    expect(c.streamPublishing()).toBe(true);

    // A background refresh asked for straight into startStream — not through setMainTab, so the
    // tab guard is not on this path and cannot be what saves the search. Today no caller does
    // this (the poll checks streaming(), the tab guard closes the re-click); the guarantee is
    // that it would not matter if one did.
    c.loadTraces('2026-01-01T00:00:00Z', undefined, 'background');
    fixture.detectChanges();

    // Refused outright: the live stream is the same object, no replacement was opened, and the
    // list was neither cut short nor stamped partial.
    expect(streams.started).toBe(started);
    expect(streams.live).toBe(searching);
    expect(c.streamPublishing()).toBe(true);        // still the user's, still stoppable
    expect(c.streamStopped()).toBe(false);
    expect(c.traces().length).toBe(rows);
    expect(el.querySelector('.stream-stop')).not.toBeNull();
    expect((el.querySelector('.list-more-hint')?.textContent ?? '').trim()).toBe('· streaming…');

    // …and it finishes as its own search, all 500 rows of it.
    for (let i = 437; i < 500; i++) streams.live.next(row('t' + i));
    streams.live.complete();
    fixture.detectChanges();
    await fixture.whenStable();
    expect(c.traces().length).toBe(500);
    expect(c.streamStopped()).toBe(false);
    expect(el.querySelector('.list-more-hint')).toBeNull();
  });

  it('B5: no stream erases the partial marker of rows it did not produce', async () => {
    const fixture = await boot([]);
    const c  = fixture.componentInstance as any;
    const el = fixture.nativeElement as HTMLElement;

    // A user search cut short by Stop: 437 rows, and a header that says they are a prefix.
    fixture.componentInstance.loadAll();
    fixture.detectChanges();
    for (let i = 0; i < 437; i++) streams.live.next(row('t' + i));
    fixture.detectChanges();
    fixture.componentInstance.stopStream();
    fixture.detectChanges();
    expect(c.traces().length).toBe(437);
    expect(c.streamStopped()).toBe(true);
    expect((el.querySelector('.list-more-hint')?.textContent ?? '').trim())
      .toBe('· stopped early — partial list, refresh to run it again');

    // A stopped list is HELD, so neither the poll nor a return to the tab touches it — but the
    // marker must survive a background stream even if one is reached, because the rows are not
    // that stream's to describe. Called directly: the guarantee is about startStream, not
    // about today's callers happening to check listHeld() first.
    const started = streams.started;
    c.loadTraces('2026-01-01T00:00:00Z', undefined, 'background');
    fixture.detectChanges();
    expect(c.streamStopped()).toBe(true);
    expect((el.querySelector('.list-more-hint')?.textContent ?? '').trim())
      .toBe('· stopped early — partial list, refresh to run it again');

    // If that stream did open, its failure restores the rows AND leaves them labelled.
    if (streams.started > started) {
      await fail(fixture);
      expect(c.traces().length).toBe(437);
      expect(c.streamStopped()).toBe(true);
      expect(c.traceqlError()).toBe('');                     // still not the user's failure
      expect((el.querySelector('.list-more-hint')?.textContent ?? '').trim())
        .toBe('· stopped early — partial list, refresh to run it again');
    }

    // The user's own next search retires the marker — together with the rows it describes.
    // That "together" is the whole rule, and it cuts both ways: the two C1 tests below are this
    // one's mirror, where a background stream DOES take the rows away and so must take the
    // marker with them. Failing is what makes this stream different: it gave the rows back.
    fixture.componentInstance.loadAll();
    fixture.detectChanges();
    expect(c.streamStopped()).toBe(false);
    expect(c.traces().length).toBe(0);
  });

  it('C1: a background stream that COMPLETES retires the marker of the rows it replaced', async () => {
    const fixture = await boot([]);
    const c  = fixture.componentInstance as any;
    const el = fixture.nativeElement as HTMLElement;

    // Same setup as B5 above: a user search stopped at 437 rows, labelled a prefix.
    fixture.componentInstance.loadAll();
    fixture.detectChanges();
    for (let i = 0; i < 437; i++) streams.live.next(row('t' + i));
    fixture.detectChanges();
    fixture.componentInstance.stopStream();
    fixture.detectChanges();
    expect(c.streamStopped()).toBe(true);

    // …and this time the background stream does not die: it delivers a whole twelve-row answer
    // of its own. Every row the marker was about is now gone from the screen, so the sentence
    // about them may not stay. Reached directly, for the same reason B5 reaches it directly —
    // the guarantee is a property of startStream, not of its callers checking listHeld() first.
    c.loadTraces('2026-01-01T00:00:00Z', undefined, 'background');
    fixture.detectChanges();
    for (let i = 0; i < 12; i++) streams.live.next(row('b' + i));
    streams.live.complete();
    fixture.detectChanges();
    await fixture.whenStable();

    // Measured before the fix: rows=12, streamStopped=true, and the header calling a complete
    // twelve-row answer a partial one.
    expect(c.traces().length).toBe(12);
    expect(c.streamStopped()).toBe(false);
    expect(c.listHeld()).toBe(false);              // …so the poll owns the list again, too
    expect(el.querySelector('.list-more-hint')).toBeNull();
  });

  it('C1: the banner is the other half of the hold, and cannot outlive its rows either', async () => {
    const fixture = await boot([]);
    const c  = fixture.componentInstance as any;
    const el = fixture.nativeElement as HTMLElement;

    // The other way a list gets held: a search the USER ran that died with five rows in hand.
    fixture.componentInstance.loadAll();
    fixture.detectChanges();
    for (let i = 0; i < 5; i++) streams.live.next(row('t' + i));
    fixture.detectChanges();
    await fail(fixture);
    expect(c.traceqlError()).toBe('Failed to load traces');
    expect(c.listHeld()).toBe(true);
    expect(el.querySelector('.tql-error')).not.toBeNull();
    expect((el.querySelector('.list-more-hint')?.textContent ?? '').trim())
      .toBe('· partial list — see the message above');

    // A background stream completes over it with an answer of its own.
    c.loadTraces('2026-01-01T00:00:00Z', undefined, 'background');
    fixture.detectChanges();
    for (let i = 0; i < 12; i++) streams.live.next(row('b' + i));
    streams.live.complete();
    fixture.detectChanges();
    await fixture.whenStable();

    // The banner explained rows that are no longer on screen; keeping it would have described
    // a failure over twelve rows that failure never produced AND frozen a list that is whole
    // and current, with nothing on screen saying why.
    expect(c.traces().length).toBe(12);
    expect(c.traceqlError()).toBe('');
    expect(c.listHeld()).toBe(false);
    expect(el.querySelector('.tql-error')).toBeNull();
    expect(el.querySelector('.list-more-hint')).toBeNull();
  });

  it('the TraceQL poll path carries the same intent — a tick is not a Run', async () => {
    const fixture = await boot([]);
    const c = fixture.componentInstance as any;

    // A query the user ran that legitimately matched nothing: complete, empty, no error.
    c.traceqlMode.set(true);
    c.traceqlInput = '{ status = error }';
    fixture.componentInstance.runTraceQLUser();
    fixture.detectChanges();
    streams.live.complete();
    fixture.detectChanges();
    await fixture.whenStable();
    expect(c.traces().length).toBe(0);
    expect(c.traceqlError()).toBe('');

    // The 15 s tick re-runs it through loadTraces → runTraceQL, and dies.
    pollTick(fixture);
    const publishingOnTick = c.streamPublishing();
    const after = await fail(fixture);
    record('empty TraceQL, tick     : ' + JSON.stringify(after));
    expect(publishingOnTick).toBe(false);
    expect(after.banner).toBe('');
    expect(after.listHeld).toBe(false);
    expect(after.bgFailures).toBe(1);
    expect(after.emptyState).toBe('No traces found');
    expect((fixture.nativeElement as HTMLElement).querySelector('.tql-error')).toBeNull();
  });

  it('a user search over a full list still clears, paints and offers Stop', async () => {
    const fixture = await boot([row('a'), row('b')]);
    const c = fixture.componentInstance as any;

    c.filterService = 'other';
    fixture.componentInstance.applyFilters();
    fixture.detectChanges();
    expect(c.traces().length).toBe(0);            // the old answer leaves with its question
    expect(c.streamPublishing()).toBe(true);

    streams.live.next(row('c'));
    fixture.detectChanges();
    expect(c.traces().length).toBe(1);            // first row paints immediately
    expect((fixture.nativeElement as HTMLElement).querySelector('.stream-stop')).not.toBeNull();

    const after = await fail(fixture);
    expect(after.banner).toBe('Failed to load traces');
    expect(after.rows).toBe(1);                   // the half-answer is kept, and explained
    expect(after.headerSuffix).toBe('· partial list — see the message above');
  });

  it('a silent tick swaps once on complete — nothing moves until it has the whole answer', async () => {
    const fixture = await boot([row('a'), row('b')]);
    const c = fixture.componentInstance as any;
    const beforeArray = c.traces();

    pollTick(fixture);
    for (let i = 0; i < 60; i++) streams.live.next(row('n' + i));   // > FlushEvery (25)
    fixture.detectChanges();
    expect(c.traces()).toBe(beforeArray);         // same array reference: nothing published

    streams.live.complete();
    fixture.detectChanges();
    await fixture.whenStable();
    expect(c.traces().length).toBe(60);
    expect(c.bgFailures()).toBe(0);
  });

  it('R6: streamMax bounds the array; the virtualizer bounds the DOM', async () => {
    const fixture = await boot([]);
    const c  = fixture.componentInstance as any;
    const el = fixture.nativeElement as HTMLElement;

    fixture.componentInstance.loadAll();
    fixture.detectChanges();
    for (let i = 0; i < c.streamMax; i++) streams.live.next(row('t' + i));
    streams.live.complete();
    fixture.detectChanges();
    await fixture.whenStable();

    expect(c.streamMax).toBe(2000);
    expect(c.traces().length).toBe(2000);
    const rendered = el.querySelectorAll('.trace-vrow').length;
    const elements = el.querySelectorAll('.trace-rows *').length;
    const spacer   = (el.querySelector('.trace-vspace') as HTMLElement | null)?.style.height;
    record(`R6: rows=${c.traces().length} rendered=${rendered} elements=${elements} `
         + `spacer=${spacer} capped=${c.streamCapped()}`);
    // The header may only claim the ceiling it was actually given, and the DOM must stay a
    // window on the array rather than a copy of it.
    expect(c.streamCapped()).toBe(true);
    expect(el.querySelector('.list-more-hint')?.textContent?.trim())
      .toBe('· newest 2000 — narrow the range or the query for older traces');
    // 600px of viewport at ~82px a row, plus 8 rows of overscan on each side.
    expect(rendered).toBeGreaterThan(0);
    expect(rendered).toBeLessThan(40);
    expect(elements).toBeLessThan(600);
    expect(spacer).toBe('164000px');              // the scrollbar still spans all 2000 rows
  });
});

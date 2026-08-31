import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { LUCIDE_ICONS, LucideIconProvider, icons, AlertCircle } from 'lucide-angular';
import { Observable, Subscriber, NEVER, of } from 'rxjs';
import { TracesComponent } from './traces';
import { ApiService } from '../../core/services/api.service';
import { TraceRowDto } from '../../core/models/span.model';

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

/** Hands out a controllable stream per call and remembers the newest one. */
class StreamController {
  private subs: Subscriber<TraceRowDto>[] = [];

  make(): Observable<TraceRowDto> {
    return new Observable<TraceRowDto>(sub => {
      this.subs.push(sub);
      return () => { this.subs = this.subs.filter(s => s !== sub); };
    });
  }

  get live(): Subscriber<TraceRowDto> {
    const s = this.subs[this.subs.length - 1];
    if (!s) throw new Error('no live stream');
    return s;
  }
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
  bgSkipTicks: number;
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
      bgSkipTicks:  c.bgSkipTicks,
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
    // Only setInterval is faked, so Angular's own setTimeout-based scheduling still runs.
    vi.useFakeTimers({ toFake: ['setInterval'] });
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
      bgFailures: 1, bgSkipTicks: 1, rows: 0,
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
      bgFailures: 1, bgSkipTicks: 1, rows: 2,
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
      bgFailures: 0, bgSkipTicks: 0, rows: 0,
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
      // Walk the backoff: bgSkipTicks ticks are swallowed before the next attempt. Read once —
      // each swallowed tick decrements it.
      const skip = c.bgSkipTicks;
      for (let s = 0; s < skip; s++) pollTick(fixture);
      expect(c.bgSkipTicks).toBe(0);
      pollTick(fixture);
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
    expect(after.headerSuffix).toBe('· stopped by an error — partial list, see the message above');
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

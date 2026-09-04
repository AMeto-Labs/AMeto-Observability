import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { LUCIDE_ICONS, LucideIconProvider, icons, AlertCircle } from 'lucide-angular';
import { Observable, Subscriber, NEVER, of } from 'rxjs';
import { TracesComponent } from './traces';
import { ApiService } from '../../core/services/api.service';
import { TraceRowDto } from '../../core/models/span.model';
import { StreamEndDto, StreamFrame } from '../../core/models/stream.model';

/**
 * Three questions about sentences, and they turn out to be one question.
 *
 * <p>WHAT MAY THE HEADER CLAIM? The server says how a stream ended — read out, stopped at the
 * row ceiling, and whether part of the window was lost getting there. The page used to derive
 * that by counting its own rows against the `max` it had asked for, which can recognise exactly
 * one of those endings and calls everything else complete.</p>
 *
 * <p>WHICH SCREEN DOES A FACT GET? Not the one its FRAME suggests. The same vanished segment is
 * reported as a `query-error` sentence when the walk reached the end of the window and as a
 * `truncatedBy` code on the terminal `done` frame when the row ceiling bit first — and the page
 * read those as two categories of news. Measured against a running server: a red banner over a
 * frozen list at max=1000, a grey count suffix over a live one at max=50, one dead file, and
 * which one the operator saw decided by how many surviving traces sat above it.</p>
 *
 * <p>WHEN MAY A WARNING BE RETIRED? Only by something that replaces the rows it is about —
 * `loadAll()` used to clear the banner ahead of a load {@link TracesComponent.loadTraces} may
 * decline outright — and, for one kind of warning, not even then. A lost segment is not a claim
 * a later read can test, so an unattended tick that simply fails to mention it has disproved
 * nothing: restart the server and the 15 s poll cleared a permanent hole off an open page.</p>
 *
 * <p>One rule underneath all three: a sentence about the list stands exactly as long as the fact
 * it is about, and an ending this page cannot name is a sentence it does not get to write.</p>
 */

class NoopResizeObserver {
  observe(): void { /* no layout in jsdom */ }
  unobserve(): void { /* no-op */ }
  disconnect(): void { /* no-op */ }
}
(globalThis as any).ResizeObserver ??= NoopResizeObserver;

// jsdom does no layout, so the virtualizer would render nothing into a 0px viewport and every
// "the header says X" assertion would pass over an empty list. Same shim as the sibling spec.
Object.defineProperty(HTMLElement.prototype, 'offsetHeight', {
  configurable: true,
  get(this: HTMLElement) { return this.classList.contains('trace-rows') ? 600 : 82; },
});

/** The server's sentence for a window it could not fully read — arrives as a query-error. */
const TRUNCATION =
  'Results are truncated: part of this window sits inside a storage segment the search ran out '
  + 'of room to open before it had to move on, so the traces it holds are missing from this list.';

/**
 * The server's RegionUnreadable sentence, verbatim from
 * `TraceQueryEndpointMapper.FinishStreamAsync` — and, word for word, the one the page writes for
 * itself when the same loss arrives as a `truncatedBy` code with no sentence attached. Asserting
 * the two are identical is what pins C5: the operator's screen may not change with the frame.
 */
const UNREADABLE =
  'Results are truncated: a storage segment inside this window could not be read — it was '
  + 'deleted or damaged — so the traces it held are missing from this list. Narrowing the time '
  + 'window will not bring them back; the server log names the file.';

class LiveStream {
  constructor(private readonly sub: Subscriber<StreamFrame<TraceRowDto>>) {}
  next(row: TraceRowDto): void { this.sub.next({ kind: 'row', row }); }
  /** `done`, with the payload the server would have sent. Omitted = an ending that explained
   *  nothing, which is what a bare `data: {}` is and what the fallback has to survive. */
  complete(end?: StreamEndDto): void {
    if (end) this.sub.next({ kind: 'end', end });
    this.sub.complete();
  }
  error(err: unknown): void { this.sub.error(err); }
}

class StreamController {
  private subs: LiveStream[] = [];
  private opened = 0;
  make(): Observable<StreamFrame<TraceRowDto>> {
    return new Observable<StreamFrame<TraceRowDto>>(sub => {
      this.opened++;
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
  get started(): number { return this.opened; }
}

function row(id: string): TraceRowDto {
  return {
    traceId: id, spanId: 's' + id, name: 'GET /x', serviceName: 'api', services: ['api'],
    status: 'Ok', httpMethod: 'GET', httpPath: '/x', httpStatusCode: 200,
    startTimeUnixNano: 1_700_000_000_000_000_000, durationNanos: 1_000_000, spanCount: 1,
  };
}

describe('the trace list header claims only what it was told', () => {
  let streams: StreamController;

  async function boot() {
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
      getServiceGraph:  () => NEVER as any,
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

    // These cases were written when the row ceiling was a fixed 2000, and several of them turn
    // on row counts staying UNDER it. The ceiling is a user-chosen setting now (default 100),
    // so it is pinned here to keep each test about its own subject rather than about the
    // default: a hint that appears because the default moved is not this suite's finding.
    (fixture.componentInstance as any).streamMax.set(2000);
    fixture.detectChanges();                        // ngOnInit → loadAll → the first stream
    await fixture.whenStable();
    streams.live.complete({ complete: true, reason: 'exhausted' });
    fixture.detectChanges();
    await fixture.whenStable();
    return fixture;
  }

  function suffix(fixture: any): string {
    const el = fixture.nativeElement as HTMLElement;
    return (el.querySelector('.list-more-hint')?.textContent ?? '').trim();
  }
  function suffixTitle(fixture: any): string {
    const el = fixture.nativeElement as HTMLElement;
    return el.querySelector('.list-more-hint')?.getAttribute('title') ?? '';
  }
  function bannerUp(fixture: any): boolean {
    return !!(fixture.nativeElement as HTMLElement).querySelector('.tql-error');
  }
  /** The standing sentence above the filter bar, whichever frame put it there. */
  function bannerText(fixture: any): string {
    const el = fixture.nativeElement as HTMLElement;
    return (el.querySelector('.tql-error')?.textContent ?? '').trim();
  }

  /** Runs a user search of `rows` rows that ends the way `end` says. */
  async function search(fixture: any, rows: number, end?: StreamEndDto) {
    fixture.componentInstance.loadAll();
    fixture.detectChanges();
    for (let i = 0; i < rows; i++) streams.live.next(row('t' + i));
    streams.live.complete(end);
    fixture.detectChanges();
    await fixture.whenStable();
  }

  // ── What the ending is allowed to become on screen ──────────────────────────────

  it('C3: a ceiling that also lost part of the window says BOTH, and says which', async () => {
    const fixture = await boot();
    const c = fixture.componentInstance as any;

    await search(fixture, 50, {
      complete: false, reason: 'max-rows', truncatedBy: 'unread-segment',
    });

    // Measured before the fix, on exactly this frame: suffix "" (50 rows is under the 2000-row
    // ceiling this page counts against), so the loss was invisible — while the same loss at a
    // different row ceiling came back as a red banner.
    expect(c.listLoss()).toBe('unread-segment');
    expect(c.listEnding()).toEqual({ kind: 'capped' });      // the ceiling is its own axis
    expect(suffix(fixture)).toBe(`· newest ${c.streamMax()}, partial list — see the message above`);
    // The advice is the reason `truncatedBy` is carried this far, so it is asserted, not
    // assumed: a segment the search ran out of room to open comes back when the window is
    // narrower. It now leads with the banner and is echoed in the tooltip.
    expect(bannerText(fixture)).toContain('Narrow the time window to bring them back into reach');
    expect(suffixTitle(fixture)).toContain('Narrow the time window');
  });

  it('C3: a segment that will NOT open is not told to narrow the window', async () => {
    const fixture = await boot();
    const c = fixture.componentInstance as any;

    await search(fixture, 50, {
      complete: false, reason: 'max-rows', truncatedBy: 'unreadable-segment',
    });

    // The two causes are deliberately different sentences because the advice is opposite —
    // telling this user to narrow their window sends them round a loop that cannot help them.
    expect(c.listLoss()).toBe('unreadable-segment');
    expect(bannerText(fixture)).toContain('Narrowing the time window will not bring them back');
    expect(suffixTitle(fixture)).toContain('NOT bring them back');
    expect(suffixTitle(fixture)).not.toContain('Narrow the time window to bring');
  });

  it('C5: one vanished segment, one screen — whichever frame the server used to say it',
    async () => {
      // THE FINDING. The server reports the SAME dead file two ways, chosen by nothing but
      // whether the row ceiling bit before the walk reached the end of the window:
      //
      //   max=1000 → query-error {"error":"Results are truncated: a storage segment …"}
      //   max=50   → done        {"complete":false,"reason":"max-rows",
      //                           "truncatedBy":"unreadable-segment"}
      //
      // streamJson routes the first to subscriber.error and the second to the ending frame, and
      // the page read those as two different categories of news. Measured before the fix:
      //   max=1000 : banner=true  held=true  hint='· stopped by an error — partial list, …'
      //   max=50   : banner=false held=false hint='· newest 2000, and part of this window …'
      // A red blocking banner over a frozen list, versus a grey count suffix over a live one,
      // for one and the same missing segment — and reachable from the product, since streamMax
      // is user-chosen (default 100) and which door the loss comes through is decided by how many surviving
      // traces sit above it.
      const fixture = await boot();
      const c = fixture.componentInstance as any;

      // ── The `done` road: the ceiling bit first, so the loss rides `truncatedBy`. ──────────
      await search(fixture, 50, {
        complete: false, reason: 'max-rows', truncatedBy: 'unreadable-segment',
      });
      const viaDone = {
        banner: bannerText(fixture), held: c.listHeld(), suffix: suffix(fixture),
      };

      // ── The `query-error` road: the same file, the same loss, the server's own sentence. ──
      // Carrying `truncatedBy`, because the server now attributes the error frame too. That is
      // what makes one treatment possible at all: before it, this road offered nothing but an
      // English sentence, so the page could not tell a lost segment from a spent deadline and
      // had to treat every error alike.
      fixture.componentInstance.loadAll();
      fixture.detectChanges();
      for (let i = 0; i < 300; i++) streams.live.next(row('t' + i));
      fixture.detectChanges();
      streams.live.error(Object.assign(new Error(UNREADABLE), { truncatedBy: 'unreadable-segment' }));
      fixture.detectChanges();
      await fixture.whenStable();
      const viaError = {
        banner: bannerText(fixture), held: c.listHeld(), suffix: suffix(fixture),
      };

      // Same sentence, word for word — the page writes the server's own when the frame that
      // carried the loss had nowhere to put one.
      expect(viaDone.banner).toBe(UNREADABLE);
      expect(viaError.banner).toBe(UNREADABLE);
      // Same treatment: a standing warning and a suffix that calls the rows partial — and a list
      // that KEEPS REFRESHING, on both roads. Holding was the old answer and it was a deadlock:
      // the poll returned early, the staleness hint was suppressed precisely while held, and the
      // failure counter could not grow behind the early return, so rows went stale for the life
      // of the page with nothing on screen saying so. A permanent hole is re-reported by the
      // server on every request, so the warning comes back on its own for as long as it is true.
      expect(viaDone.held).toBe(false);
      expect(viaError.held).toBe(false);
      expect(viaDone.suffix).toContain('partial list — see the message above');
      expect(viaError.suffix).toContain('partial list — see the message above');
      // The one thing that may still differ is DETAIL only one road has: the `done` road knows
      // the ceiling ALSO bit. Extra facts are not a different screen.
      expect(viaDone.suffix).toBe(`· newest ${c.streamMax()}, partial list — see the message above`);
      expect(viaError.suffix).toBe('· partial list — see the message above');
    });

  it('C5: the minor — the hint names the ceiling the REQUEST carried, not the next one',
    async () => {
      const fixture = await boot();
      const c = fixture.componentInstance as any;

      // The hint interpolated `this.streamMax`, so it read "newest 2000" whatever was asked.
      // Invisible while every caller sends the same number, which is why it is pinned by
      // driving a stream that asked for something else.
      c.startStream(streams.make(), 'user', 25);
      fixture.detectChanges();
      for (let i = 0; i < 25; i++) streams.live.next(row('t' + i));
      streams.live.complete({ complete: false, reason: 'max-rows' });
      fixture.detectChanges();
      await fixture.whenStable();

      expect(c.listMax()).toBe(25);
      expect(suffix(fixture)).toBe('· newest 25 — narrow the range or the query for older traces');

      // …and the other sentence that names a ceiling: the one a loss puts on top of it. Two
      // interpolations, two chances to reach for the wrong number.
      c.startStream(streams.make(), 'user', 25);
      fixture.detectChanges();
      for (let i = 0; i < 25; i++) streams.live.next(row('u' + i));
      streams.live.complete({
        complete: false, reason: 'max-rows', truncatedBy: 'unreadable-segment',
      });
      fixture.detectChanges();
      await fixture.whenStable();
      expect(suffix(fixture)).toBe('· newest 25, partial list — see the message above');
    });

  it('C3: the plain ceiling keeps the sentence it always had', async () => {
    const fixture = await boot();
    const c = fixture.componentInstance as any;

    await search(fixture, 50, { complete: false, reason: 'max-rows' });

    // 50 rows, and the header says the ceiling stopped it — because the SERVER said so. The
    // row count cannot reach this conclusion at all, which is the point.
    expect(c.streamCapped()).toBe(true);
    expect(suffix(fixture)).toBe(`· newest ${c.streamMax()} — narrow the range or the query for older traces`);
  });

  it('C3: a window read out is the only thing a bare count is allowed to mean', async () => {
    const fixture = await boot();
    const c = fixture.componentInstance as any;

    await search(fixture, 50, { complete: true, reason: 'exhausted' });

    expect(c.listEnding()).toEqual({ kind: 'read-out' });
    expect(c.streamCapped()).toBe(false);
    expect(suffix(fixture)).toBe('');
  });

  // ── Endings this client cannot name ────────────────────────────────────────────

  it('C3: an unreadable ending degrades to the row count, and never upgrades to complete',
    async () => {
      const fixture = await boot();
      const c = fixture.componentInstance as any;

      // The two log streams still end with a bare `data: {}`, and a future server may say
      // something new. Neither may be read as an endorsement.
      await search(fixture, c.streamMax(), {});
      expect(c.streamCapped()).toBe(true);
      expect(suffix(fixture)).toBe(`· newest ${c.streamMax()} — narrow the range or the query for older traces`);

      await search(fixture, c.streamMax(), { complete: false, reason: 'some-future-ending' });
      expect(c.streamCapped()).toBe(true);

      // …and an unrecognised `truncatedBy` degrades the same way: the ceiling is still
      // reported, the part this page cannot describe is not described.
      await search(fixture, 50, { complete: false, reason: 'max-rows', truncatedBy: 'moon-phase' });
      expect(c.listEnding()).toEqual({ kind: 'capped' });
      expect(suffix(fixture)).toBe(`· newest ${c.streamMax()} — narrow the range or the query for older traces`);
    });

  it('C3: an ending that denies completeness is reported short even when unexplained', async () => {
    const fixture = await boot();
    const c = fixture.componentInstance as any;

    // complete:false with a reason from a server newer than this page, under the ceiling. The
    // row count cannot see it, and falling through to a bare count would repeat the server's
    // own denial back to the user as consent.
    await search(fixture, 50, { complete: false, reason: 'deadline' });

    expect(c.listEnding()).toEqual({ kind: 'short' });
    expect(suffix(fixture)).toBe('· partial list — the server stopped before the end of the window');
  });

  it('C3: an ending belongs to its own stream — a superseded one cannot label the next list',
    async () => {
      const fixture = await boot();
      const c = fixture.componentInstance as any;

      await search(fixture, 50, { complete: false, reason: 'max-rows', truncatedBy: 'unread-segment' });
      expect(c.streamCapped()).toBe(true);

      // A new search takes the list. The old sentence goes out with the rows it was about, at
      // the moment they are emptied — not when the new answer happens to arrive.
      fixture.componentInstance.loadAll();
      fixture.detectChanges();
      expect(c.listEnding()).toBeNull();
      expect(suffix(fixture)).toBe('· streaming…');

      streams.live.complete({ complete: true, reason: 'exhausted' });
      fixture.detectChanges();
      await fixture.whenStable();
      expect(c.listEnding()).toEqual({ kind: 'read-out' });
      expect(suffix(fixture)).toBe('');
    });

  // ── C4: a warning is replaced, never retired in advance ────────────────────────

  it('C4: Refresh over a truncation — the warning is only ever off while the answer is pending',
    async () => {
      const fixture = await boot();
      const c = fixture.componentInstance as any;

      // A search the USER ran that the server truncated: rows, a banner, a header that calls
      // them partial, and a held list so the poll cannot take them away.
      fixture.componentInstance.loadAll();
      fixture.detectChanges();
      for (let i = 0; i < 300; i++) streams.live.next(row('t' + i));
      fixture.detectChanges();
      streams.live.error(new Error(TRUNCATION));
      fixture.detectChanges();
      await fixture.whenStable();
      expect(bannerUp(fixture)).toBe(true);
      expect(c.listHeld()).toBe(true);
      expect(suffix(fixture)).toBe('· partial list — see the message above');

      // Refresh — the control that sits next to the banner.
      const started = streams.started;
      fixture.componentInstance.loadAll();
      fixture.detectChanges();
      expect(streams.started).toBe(started + 1);      // a real re-run, not a no-op clear

      // The banner is down, and it is down because the rows it described are GONE in the same
      // step. Nothing here reads as "fixed": there is no list, and the header says a search is
      // running. That is the only state in which the warning is allowed to be absent.
      expect(bannerUp(fixture)).toBe(false);
      expect(c.traces().length).toBe(0);
      expect(suffix(fixture)).toBe('· streaming…');

      for (let i = 0; i < 300; i++) streams.live.next(row('t' + i));
      fixture.detectChanges();
      expect(suffix(fixture)).toBe('· streaming…');   // still pending, still not a claim

      // The server reports the same truncation, and the warning is back with its rows.
      streams.live.error(new Error(TRUNCATION));
      fixture.detectChanges();
      await fixture.whenStable();
      expect(bannerUp(fixture)).toBe(true);
      expect(c.traceqlError()).toBe(TRUNCATION);
      expect(c.listHeld()).toBe(true);
      expect(suffix(fixture)).toBe('· partial list — see the message above');
    });

  it('C4: a click that starts no query retires nothing — the warning waits for the answer',
    async () => {
      const fixture = await boot();
      const c  = fixture.componentInstance as any;

      fixture.componentInstance.loadAll();
      fixture.detectChanges();
      for (let i = 0; i < 300; i++) streams.live.next(row('t' + i));
      fixture.detectChanges();
      streams.live.error(new Error(TRUNCATION));
      fixture.detectChanges();
      await fixture.whenStable();
      expect(c.listHeld()).toBe(true);

      // The range chips render ABOVE the tab strip, so they stay live while Service Graph is
      // showing — and the list refuses to stream into a panel that is off screen.
      fixture.componentInstance.setMainTab('graph');
      fixture.detectChanges();
      const started = streams.started;
      const rows    = c.traces();
      fixture.componentInstance.setPreset('6h');
      fixture.detectChanges();

      // Measured before the fix: held=false, banner gone, rows untouched — a warning retired by
      // a click that started no query and changed no row, over the very rows it was about.
      expect(streams.started).toBe(started);          // nothing ran
      expect(c.traces()).toBe(rows);                  // nothing moved
      expect(c.traceqlError()).toBe(TRUNCATION);      // …so nothing may be retired
      expect(c.listHeld()).toBe(true);

      // Coming back runs the load the hidden list could not — as the USER's, because that is
      // whose click it was. It empties the list and retires the marker in the same step, and it
      // owns its own failure.
      fixture.componentInstance.setMainTab('traces');
      fixture.detectChanges();
      expect(streams.started).toBe(started + 1);
      expect(c.traceqlError()).toBe('');
      expect(c.traces().length).toBe(0);
      expect(c.streamPublishing()).toBe(true);        // the user's search, stoppable

      streams.live.error(new Error(TRUNCATION));
      fixture.detectChanges();
      await fixture.whenStable();
      expect(bannerUp(fixture)).toBe(true);           // and a background tick could not say this
      expect(c.bgFailures()).toBe(0);
    });

  it('C4: the remembered load is spent once, and a return to the tab is still a poll after it',
    async () => {
      const fixture = await boot();
      const c = fixture.componentInstance as any;

      fixture.componentInstance.setMainTab('graph');
      fixture.detectChanges();
      fixture.componentInstance.setPreset('6h');       // remembered
      fixture.detectChanges();

      fixture.componentInstance.setMainTab('traces');  // …and spent
      fixture.detectChanges();
      expect(c.streamPublishing()).toBe(true);
      streams.live.complete({ complete: true, reason: 'exhausted' });
      fixture.detectChanges();
      await fixture.whenStable();

      // Leaving and returning again is navigation, not a search: it must be a background
      // refresh, or a stale flag would make every later return own its failures as the user's.
      fixture.componentInstance.setMainTab('graph');
      fixture.detectChanges();
      fixture.componentInstance.setMainTab('traces');
      fixture.detectChanges();
      expect(c.streamPublishing()).toBe(false);
      streams.live.error(new Error('Failed to load traces'));
      fixture.detectChanges();
      await fixture.whenStable();
      expect(c.traceqlError()).toBe('');               // nobody asked, so nobody is told
      expect(c.bgFailures()).toBe(1);
    });

  // ── C6: confidence travels one way on a background tick ────────────────────────
  //
  // C4 established that a warning is retired by whatever replaces the rows it is about — and a
  // background stream that COMPLETES does replace them, so it retires them. That is right for
  // every marker which describes ONE READ: "the user stopped this one", "this one died". It is
  // wrong for exactly one marker, and the difference is not a preference about loudness.
  //
  // A lost segment is not a claim a later read can test. Nothing re-reads a file that is gone —
  // the server says so where it remembers them (VanishedRegionLog) — so the only ways it stops
  // being re-reported are the two that prove nothing: the process restarts and forgets, or
  // retention walks past the range. Both leave the hole exactly where it was.

  it('C6: the loss marker describes the rows on screen, and follows them when they change',
    async () => {
      // THIS TEST USED TO ASSERT THE OPPOSITE, and the reversal is deliberate.
      //
      // The old rule was raise-only: a background tick could add a loss but never retire one,
      // because the server's memory of a vanished segment was a field in a process and a restart
      // made the next tick call the window whole while the traces were still missing. That rule
      // came with a HOLD — nothing under the banner could change — so the marker always described
      // what was on screen.
      //
      // The hold is gone: freezing a live list over a fact about storage was a deadlock with no
      // indicator. Raise-only without the hold is worse than either rule on its own, because the
      // page then holds the server's positive claim that the window was read out AND a banner
      // calling the rows partial — over rows the claim is not even about.
      //
      // What protects the warning now is the server rather than the page: the engine remembers a
      // vanished segment, so a real hole is re-reported on the very next request. A marker that
      // outlives its rows is a lie on screen immediately; one that waits a poll interval for the
      // server to repeat itself is not.
      const fixture = await boot();
      const c = fixture.componentInstance as any;

      await search(fixture, 300, {
        complete: false, reason: 'max-rows', truncatedBy: 'unreadable-segment',
      });
      expect(c.listLoss()).toBe('unreadable-segment');
      expect(bannerText(fixture)).toBe(UNREADABLE);

      // A tick that brings different rows brings its own account of them.
      c.loadTraces('2026-01-01T00:00:00Z', undefined, 'background');
      fixture.detectChanges();
      for (let i = 0; i < 12; i++) streams.live.next(row('b' + i));
      streams.live.complete({ complete: true, reason: 'exhausted' });
      fixture.detectChanges();
      await fixture.whenStable();

      expect(c.traces().length).toBe(12);
      expect(c.traces()[0].traceId).toBe('b0');
      expect(c.listLoss()).toBeNull();          // …and no warning left over the rows it replaced
      expect(bannerUp(fixture)).toBe(false);

      // And the server saying it again is what brings it back — which is the protection the page
      // gave up, relocated to where the fact actually lives.
      c.loadTraces('2026-01-01T00:00:00Z', undefined, 'background');
      fixture.detectChanges();
      for (let i = 0; i < 12; i++) streams.live.next(row('c' + i));
      streams.live.complete({
        complete: false, reason: 'max-rows', truncatedBy: 'unreadable-segment',
      });
      fixture.detectChanges();
      await fixture.whenStable();
      expect(c.listLoss()).toBe('unreadable-segment');
      expect(bannerText(fixture)).toBe(UNREADABLE);
    });

  it('C6: the user asking again is what retires it — and a recovered window comes back clean',
    async () => {
      const fixture = await boot();
      const c = fixture.componentInstance as any;

      await search(fixture, 50, {
        complete: false, reason: 'max-rows', truncatedBy: 'unread-segment',
      });
      expect(c.listLoss()).toBe('unread-segment');

      // Refresh — the control sitting right beside the banner. It empties the list, so the
      // warning goes out with the rows it was about, and the answer decides what replaces it.
      fixture.componentInstance.loadAll();
      fixture.detectChanges();
      expect(c.listLoss()).toBeNull();
      expect(bannerUp(fixture)).toBe(false);
      expect(suffix(fixture)).toBe('· streaming…');          // pending, so claiming nothing

      // The server still reports the hole → the warning is back with its rows.
      for (let i = 0; i < 50; i++) streams.live.next(row('t' + i));
      streams.live.complete({ complete: false, reason: 'max-rows', truncatedBy: 'unread-segment' });
      fixture.detectChanges();
      await fixture.whenStable();
      expect(c.listLoss()).toBe('unread-segment');

      // And the recovery story, which is the same door: the user asks again over a window that
      // really is whole — a narrower range, a different filter, or simply after the operator
      // restored the file — and this time nothing reports a loss, so nothing claims one.
      fixture.componentInstance.loadAll();
      fixture.detectChanges();
      for (let i = 0; i < 12; i++) streams.live.next(row('r' + i));
      streams.live.complete({ complete: true, reason: 'exhausted' });
      fixture.detectChanges();
      await fixture.whenStable();
      expect(c.listLoss()).toBeNull();
      expect(c.listHeld()).toBe(false);                      // …and the poll owns the list again
      expect(bannerUp(fixture)).toBe(false);
      expect(suffix(fixture)).toBe('');
    });

  it('C6: one way only — a background tick may still make the page LESS confident', async () => {
    const fixture = await boot();
    const c = fixture.componentInstance as any;

    // A clean, complete list nobody has any warning about.
    await search(fixture, 12, { complete: true, reason: 'exhausted' });
    expect(c.listLoss()).toBeNull();
    expect(c.listHeld()).toBe(false);

    // The tick is the first to meet the dead segment. Withholding that because "nobody asked"
    // would be the mirror of the bug above — the rule is about raising confidence, not about
    // who is allowed to lower it.
    c.loadTraces('2026-01-01T00:00:00Z', undefined, 'background');
    fixture.detectChanges();
    for (let i = 0; i < 50; i++) streams.live.next(row('b' + i));
    streams.live.complete({ complete: false, reason: 'max-rows', truncatedBy: 'unreadable-segment' });
    fixture.detectChanges();
    await fixture.whenStable();

    expect(c.listLoss()).toBe('unreadable-segment');
    expect(bannerText(fixture)).toBe(UNREADABLE);
    // The loss stands, and the list stays LIVE. Freezing it here was the old answer and it was
    // a deadlock with no indicator: the poll returned early, the staleness hint was suppressed
    // exactly while held, and the failure counter could not grow behind the early return. What
    // keeps the warning honest instead is the server, which re-reports a permanent hole on
    // every request — so it persists while it is true and lifts when it stops being.
    expect(c.listHeld()).toBe(false);
  });
});

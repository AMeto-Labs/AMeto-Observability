import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { LUCIDE_ICONS, LucideIconProvider, icons, AlertCircle } from 'lucide-angular';
import { Observable, Subscriber, NEVER, of } from 'rxjs';
import { TracesComponent } from './traces';
import { ApiService } from '../../core/services/api.service';
import { TraceRowDto } from '../../core/models/span.model';
import { StreamEndDto, StreamFrame } from '../../core/models/stream.model';

/**
 * The Live control: a switch and a cadence menu over ONE signal.
 *
 * <p>The page used to poll at 15 s, full stop, and three of the four things that made that safe
 * were written in terms of that one number — the burst floor under it, the backoff ladder
 * measured in it, and the sentence the header shows when the refresh stops working. Making the
 * cadence a choice is therefore not "pass a variable to setInterval": a 5 s floor over a 1 s
 * cadence is a menu entry that lies, a backoff counted in 1 s intervals re-fires a doomed query
 * eight times a minute, and "the 15-second live refresh keeps failing" is a wrong number rather
 * than a stale one. This file is those seams, plus the two states the control itself has to keep
 * straight: off is off for BOTH callers of poll(), and resuming resumes what was paused.</p>
 */

class NoopResizeObserver {
  observe(): void { /* no layout in jsdom */ }
  unobserve(): void { /* no-op */ }
  disconnect(): void { /* no-op */ }
}
(globalThis as any).ResizeObserver ??= NoopResizeObserver;

Object.defineProperty(HTMLElement.prototype, 'offsetHeight', {
  configurable: true,
  get(this: HTMLElement) { return this.classList.contains('trace-rows') ? 600 : 82; },
});

class LiveStream {
  constructor(private readonly sub: Subscriber<StreamFrame<TraceRowDto>>) {}
  next(row: TraceRowDto): void { this.sub.next({ kind: 'row', row }); }
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

describe('traces — the live cadence is a choice, and the poll is built out of it', () => {
  let streams: StreamController;
  /** Stats are the half of a tick that runs even when the list declines to, so they are how a
   *  tick is counted when the list is streaming, held, or off screen. */
  let statsCalls: number;

  async function boot() {
    // Two components in one test (the storage round-trip) need the module back.
    TestBed.resetTestingModule();
    streams = new StreamController();
    statsCalls = 0;
    const api: Partial<ApiService> = {
      streamTraceList:  () => streams.make(),
      streamTraceQuery: () => streams.make(),
      getTraceStats:    () => { statsCalls++; return NEVER as any; },
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

    // The page's first load delivers one complete row, so the list is neither empty nor held
    // and every later stream in this file is a background refresh.
    streams.live.next(row('a'));
    streams.live.complete();
    fixture.detectChanges();
    await fixture.whenStable();
    return fixture;
  }

  async function advance(fixture: any, ms: number): Promise<void> {
    vi.advanceTimersByTime(ms);
    fixture.detectChanges();
    await fixture.whenStable();
  }

  beforeEach(() => {
    // Same fake-clock shape as the background-intent spec: the backoff is a wall-clock deadline,
    // so Date moves with the interval or the ladder would be "passed" by standing still.
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval', 'Date'] });
    // The chosen cadence is persisted, and one jsdom serves the whole file — without this, the
    // test that picks 5 s decides the default for every test after it.
    localStorage.clear();
  });
  afterEach(() => vi.useRealTimers());

  it('the default is the cadence the page always had — 15 s, and nothing at 14', async () => {
    const fixture = await boot();
    const c = fixture.componentInstance as any;
    expect(c.liveMs()).toBe(15_000);
    expect(c.liveOn()).toBe(true);

    await advance(fixture, 14_000);
    expect(statsCalls).toBe(1);                    // the boot load's, and no tick yet
    expect(streams.started).toBe(1);

    await advance(fixture, 1_000);
    expect(statsCalls).toBe(2);
    expect(streams.started).toBe(2);
  });

  it('1 s means 1 s: the 5 s burst floor is capped by the cadence, not applied over it',
    async () => {
      const fixture = await boot();
      const c = fixture.componentInstance as any;
      c.setLiveMs(1_000);
      fixture.detectChanges();

      // The regression this pins: with a fixed 5 s floor the first tick got through and the
      // next four were swallowed, so the menu said 1s and the page ran at 5s.
      await advance(fixture, 1_000);
      expect(streams.started).toBe(2);
      // …and the tick's stream is still open, so the LIST stands back while it delivers —
      // stats are what proves the clock itself is still running at 1 s.
      await advance(fixture, 1_000);
      await advance(fixture, 1_000);
      expect(statsCalls).toBe(4);                  // boot + three ticks
      expect(streams.started).toBe(2);             // one open stream is not replaced mid-flight
    });

  it('Off stops the timer AND the other caller: alt-tabbing a paused page fetches nothing',
    async () => {
      const fixture = await boot();
      const c = fixture.componentInstance as any;

      c.setLiveMs(0);
      fixture.detectChanges();
      expect(c.liveOn()).toBe(false);
      expect(c.liveLabel()).toBe('Off');

      await advance(fixture, 120_000);
      // The visibility handler has no timer to have been cleared — it calls poll() directly.
      document.dispatchEvent(new Event('visibilitychange'));
      await advance(fixture, 0);

      expect(statsCalls).toBe(1);                  // still just the boot load's
      expect(streams.started).toBe(1);
    });

  it('the switch resumes what it paused — the cadence, not the default — and refreshes at once',
    async () => {
      const fixture = await boot();
      const c = fixture.componentInstance as any;
      c.setLiveMs(30_000);
      fixture.detectChanges();

      c.toggleLive();                              // pause
      fixture.detectChanges();
      expect(c.liveMs()).toBe(0);
      await advance(fixture, 90_000);
      expect(streams.started).toBe(1);

      c.toggleLive();                              // resume
      fixture.detectChanges();
      await fixture.whenStable();
      expect(c.liveMs()).toBe(30_000);             // NOT 15 s: pausing must not change a setting
      // Resuming asks for fresh rows now, rather than up to 30 s from now.
      expect(streams.started).toBe(2);
    });

  it('pausing retires a failing run: no "refresh failing" over rows the user froze', async () => {
    const fixture = await boot();
    const c = fixture.componentInstance as any;

    // Three background failures, walking the backoff ladder out (15 s → 30 s → 60 s) rather
    // than assuming it: the tick that gets through is the one that opens a stream.
    for (let i = 0; i < 3; i++) {
      const before = streams.started;
      for (let step = 0; step < 12 && streams.started === before; step++) {
        await advance(fixture, 15_000);
      }
      expect(streams.started).toBe(before + 1);
      streams.live.error(new Error('Failed to load traces'));
      fixture.detectChanges();
      await fixture.whenStable();
    }

    expect(c.bgFailures()).toBe(3);
    expect(c.bgRefreshFailing()).toBe(true);
    expect(c.traces().length).toBe(1);             // the complete answer was kept, as ever

    c.setLiveMs(0);
    fixture.detectChanges();
    // Nothing is refreshing, so nothing is failing to refresh — and the quarantine that was
    // running is gone, so resuming is not a resume that visibly does nothing for two minutes.
    expect(c.bgRefreshFailing()).toBe(false);
    expect(c.bgFailures()).toBe(0);
    expect(c.nextBgListAt).toBe(0);
  });

  it('the backoff is floored at the default, so a 1 s cadence cannot hammer a doomed query',
    async () => {
      const fixture = await boot();
      const c = fixture.componentInstance as any;
      c.setLiveMs(1_000);
      fixture.detectChanges();

      await advance(fixture, 1_000);
      expect(streams.started).toBe(2);
      streams.live.error(new Error('Failed to load traces'));
      fixture.detectChanges();
      await fixture.whenStable();

      // One failure quarantines the list for a backoff unit — 15 s, not the 1 s the ticks run
      // at. The ticks keep coming (stats still refresh); the failing query does not.
      expect(c.nextBgListAt - Date.now()).toBe(15_000);
      await advance(fixture, 5_000);
      expect(streams.started).toBe(2);
      await advance(fixture, 10_000);
      expect(streams.started).toBe(3);
    });

  it('the chosen cadence outlives the component: stored, and restored by the next one',
    async () => {
      const first = await boot();
      (first.componentInstance as any).setLiveMs(5_000);
      first.detectChanges();
      expect(localStorage.getItem('ameto-traces-live-ms')).toBe('5000');

      const second = await boot();
      const c = second.componentInstance as any;
      expect(c.liveMs()).toBe(5_000);
      // And it is live at that cadence, not merely displaying it.
      await advance(second, 5_000);
      expect(streams.started).toBe(2);
    });

  it('the switch and the menu render ONE state, in both directions', async () => {
    const fixture = await boot();
    const c  = fixture.componentInstance as any;
    const el = fixture.nativeElement as HTMLElement;
    const sel = () => el.querySelector('select.live-select') as HTMLSelectElement;
    const btn = () => el.querySelector('button.live-toggle') as HTMLButtonElement;
    const group = () => el.querySelector('.live-group') as HTMLElement;

    expect(btn().textContent!.trim()).toBe('Live');
    expect(btn().getAttribute('aria-checked')).toBe('true');
    expect(sel().selectedOptions[0].textContent!.trim()).toBe('15s');

    // The switch moves the menu…
    c.toggleLive();
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();
    expect(btn().textContent!.trim()).toBe('Paused');
    expect(btn().getAttribute('aria-checked')).toBe('false');
    expect(group().classList.contains('off')).toBe(true);
    expect(sel().selectedOptions[0].textContent!.trim()).toBe('Off');

    // …and the menu moves the switch.
    c.setLiveMs(10_000);
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();
    expect(btn().textContent!.trim()).toBe('Live');
    expect(group().classList.contains('off')).toBe(false);
    expect(sel().selectedOptions[0].textContent!.trim()).toBe('10s');
  });

  it('the staleness tooltip names the cadence in force, not the one that used to be fixed',
    async () => {
      const fixture = await boot();
      const c = fixture.componentInstance as any;
      expect(c.bgRefreshFailingTitle()).toContain('The 15s live refresh');
      c.setLiveMs(3_000);
      expect(c.bgRefreshFailingTitle()).toContain('The 3s live refresh');
    });
});

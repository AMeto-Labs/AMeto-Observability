import {
  Component, signal, computed, inject, OnInit, ElementRef,
  OnDestroy, HostListener, ChangeDetectionStrategy, ChangeDetectorRef,
  viewChild, viewChildren, afterRenderEffect,
} from '@angular/core';
import { injectVirtualizer } from '@tanstack/angular-virtual';
import { Observable, Subscription } from 'rxjs';
import { Router, ActivatedRoute } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import { format } from 'date-fns';
import { ApiService } from '../../core/services/api.service';
import { serviceColor } from '../../shared/utils/service-color';
import { EventDto } from '../../core/models/event.model';
import { SpanDto, TraceRowDto, TraceStatsDto } from '../../core/models/span.model';
import { subHours, formatISO } from 'date-fns';
import { ServiceGraphComponent } from './service-graph/service-graph';
import { FlamegraphComponent } from './flame-graph/flame-graph';
import { LatencyComponent } from './latency/latency';
import { CompareTraceComponent } from './compare-trace/compare-trace';
import { SuggestInputDirective } from '../../shared/suggest/suggest-input.directive';
import { ModalComponent } from '../../shared/components/ui';
import { EventDetailComponent } from '../events/components/event-detail/event-detail';
import { EventListRowComponent } from '../events/components/event-list-row/event-list-row';
import { PropertyMenuComponent } from '../../shared/components/property-menu/property-menu';
import { SearchHistoryComponent } from '../../shared/components/search-history/search-history';
import { SearchHistoryService } from '../../core/services/search-history.service';

/**
 * Who asked for a load of the trace list.
 *
 * It answers ONE question — may this stream own the failure? — and it is deliberately not the
 * answer to "may this stream paint as it goes?", which is a question about the rows on screen.
 * The two were once the same expression (`inPlace && traces().length > 0`), and an empty list
 * made them disagree: the 15 s poll took the user's branch, so one flaky tick raised a banner
 * for a search nobody ran, offered Stop on an unattended refresh, kept the background failure
 * counter (and its backoff) at zero, and froze the list for the life of the page.
 *
 * 'background' is the poll's silent refresh and the return to the Traces tab — both re-run the
 * query the list is ALREADY showing. Everything else is 'user': Apply, Run, Refresh, Clear, a
 * range chip, a seek from the property menu, and the page's first load.
 */
export type LoadOrigin = 'user' | 'background';

/** TraceQL vocabulary offered by the Ctrl+Space autocomplete: intrinsics, common OTel span
 *  attributes (dotted), status/kind enum values, and the comparison/boolean operators. */
const TRACEQL_TOKENS: readonly string[] = [
  // intrinsics
  'status', 'duration', 'name', 'service', 'kind',
  // common span / resource attributes
  '.http.status_code', '.http.request.method', '.http.route', '.http.target', '.http.url',
  '.http.response.status_code', '.rpc.method', '.rpc.service', '.db.system', '.db.statement',
  '.db.name', '.net.peer.name', '.messaging.system', '.error',
  // enum values
  'error', 'ok', 'unset',
  'server', 'client', 'producer', 'consumer', 'internal',
  // operators / duration units
  '&&', '||', '=', '!=', '>', '>=', '<', '<=', 'ms', 's',
];

@Component({
  selector: 'app-traces',
  imports: [FormsModule, LucideAngularModule, ServiceGraphComponent, FlamegraphComponent, LatencyComponent, CompareTraceComponent, SuggestInputDirective, ModalComponent, EventDetailComponent, EventListRowComponent, PropertyMenuComponent, SearchHistoryComponent],
  templateUrl: './traces.html',
  styleUrl: './traces.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class TracesComponent implements OnInit, OnDestroy {
  private api    = inject(ApiService);
  private cdr    = inject(ChangeDetectorRef);
  private router = inject(Router);
  private route  = inject(ActivatedRoute);
  /** This page's slice of the shared per-user search history (scope keeps it apart
   *  from the Events/Metrics lists — see SearchHistoryService.forScope). */
  private history = inject(SearchHistoryService).forScope('traces');

  // ── Loading / data ────────────────────────────────────────────────────────
  statsLoading  = signal(false);
  stats         = signal<TraceStatsDto | null>(null);
  loading       = signal(false);
  traces        = signal<TraceRowDto[]>([]);

  selectedTraceId = signal<string | null>(null);
  traceSpans      = signal<SpanDto[]>([]);
  traceLoading    = signal(false);
  selectedSpan    = signal<SpanDto | null>(null);

  activeMainTab:  'traces' | 'graph' | 'latency' | 'compare' = 'traces';
  activeTraceTab: 'timeline' | 'flamegraph' | 'details' = 'timeline';
  activeSpanTab:  'tags' | 'logs' = 'tags';

  // Trace logs (correlated by @tr — primary trace↔logs view).
  // Loaded once per trace; the selected span narrows them client-side.
  traceLogs        = signal<EventDto[]>([]);
  traceLogsLoading = signal(false);
  traceLogsLoaded  = signal(false);
  onlyThisSpan     = signal(false);

  /** Log opened in the full-detail modal (the same renderer the Events page uses); null = closed. */
  logModalEvent    = signal<EventDto | null>(null);

  /** Logs shown in the Logs tab: all trace logs, or only those of the selected span. */
  visibleLogs = computed(() => {
    const all = this.traceLogs();
    if (!this.onlyThisSpan()) return all;
    const sp = this.selectedSpan()?.spanId;
    if (!sp) return all;
    return all.filter(l => l['@sp'] === sp);
  });

  /** How many logs belong to the currently selected span (for the badge). */
  spanLogCount = computed(() => {
    const sp = this.selectedSpan()?.spanId;
    if (!sp) return 0;
    return this.traceLogs().reduce((n, l) => n + (l['@sp'] === sp ? 1 : 0), 0);
  });

  // ── Filters ───────────────────────────────────────────────────────────────
  traceIdInput       = '';
  filterName         = '';
  filterService      = '';
  filterStatus       = '';
  filterMinDurationMs: number | null = null;
  filterMaxDurationMs: number | null = null;
  filterHttpStatus   = '';
  preset             = '1h';
  /** Custom [from,to] as datetime-local strings — used when preset === 'custom'. */
  customFrom         = '';
  customTo           = '';

  /** Stable query window driving the Graph / Latency panels. Set on user action only, so the
   *  15 s live poll never changes it → those panels don't re-fetch/re-layout (no jumping). */
  readonly winFrom     = signal<string>('');
  readonly winTo       = signal<string | undefined>(undefined);
  /** Full service list from the backend (not just services present in loaded traces). */
  readonly allServices = signal<string[]>([]);

  // TraceQL
  traceqlInput   = '';
  traceqlMode    = signal(false);
  /** Why the last search the USER ran ended in an error — '' when it did not. Despite the
   *  name it is not TraceQL-specific: the filter-bar path streams too, and dies the same ways.
   *  It does three jobs at once, and that is the point — one signal cannot contradict itself.
   *  It is the banner text above the filter bar; it is the list-header suffix that stops a
   *  half-delivered answer reading as a finished one; and it is half of listHeld, so the list
   *  thaws exactly when the explanation for freezing it leaves the screen. A failing BACKGROUND
   *  refresh must never set it: see the error branch of startStream. */
  traceqlError   = signal('');
  /** Candidates for the TraceQL Ctrl+Space autocomplete. */
  readonly traceqlSuggestions = TRACEQL_TOKENS as string[];

  /** Right-hand search-history panel — mounted under @if so opening reloads. */
  historyOpen = signal(false);

  private _poll: ReturnType<typeof setInterval> | null = null;

  // ── Streaming ─────────────────────────────────────────────────────────────
  // The list arrives over SSE, one row at a time. There is no request epoch here and no
  // page bookkeeping: the epoch counter existed only because a slow response from an
  // ABANDONED search could splice itself into the list of the search that replaced it, and
  // unsubscribing the previous stream before starting the next one is that same guarantee
  // made structural — a cancelled EventSource has no callbacks left to fire. Deleted, not
  // forgotten.

  /** The one stream that may own the list. Non-null exactly while rows can still arrive. */
  private streamSub: Subscription | null = null;
  /** Publishes the current stream's buffer into `traces`, or null while the stream has
   *  nothing it may hand over yet (a silent refresh — see startStream). */
  private flushPending: (() => void) | null = null;
  /** True while ANY stream is open: the poll stands back (never two at once) and the Refresh
   *  icon spins, which is the whole visible footprint of a background refresh. */
  readonly streaming = signal(false);
  /** True while the open stream is one the USER asked for. The "streaming…" hint and the Stop
   *  button belong to this one: a background refresh has nothing the user needs to interrupt,
   *  and offering Stop twice a minute is just flicker — including over an empty list, which is
   *  a fact about the rows and never about who asked. What it is NOT is "this stream reaches
   *  the rows": a background refresh of an empty list does paint into it (streamPaints), and
   *  that is the flag stopStream reads. */
  readonly streamPublishing = signal(false);
  /** Whether the open stream may put rows on screen at all — true for every user-initiated
   *  search and for a background refresh that found the list empty. Read by stopStream: only
   *  a stream that was painting can leave a PARTIAL list behind, and only then may the header
   *  call it partial. Not a signal: nothing renders it, it only qualifies the stamp. */
  private streamPaints = false;
  /** True when the last stream ended because it hit `streamMax`, so older traces exist
   *  beyond what is on screen and the header must say so rather than imply completeness. */
  readonly streamCapped = signal(false);
  /** True when the last stream was cut short — Stop, or leaving the list mid-flight. The rows
   *  on screen are then a PREFIX of the answer, and the header has to say so: stopped at 300
   *  of a window holding 2000 must not read like a search that genuinely found 300. */
  readonly streamStopped = signal(false);
  /** The list is the user's, not the poll's: they stopped it, or a search THEY ran died and
   *  left them the rows it found plus the banner explaining why that is all there is. Either
   *  way the 15 s refresh leaves both alone until the user runs something — taking the rows
   *  back mid-read is the same theft whichever way the stream ended.
   *
   *  Both halves are things the user did and can see. A BACKGROUND refresh that fails is
   *  neither, and deliberately does not reach here: it would freeze the list over a query the
   *  user never ran, with nothing on screen saying so. It backs off instead (bgSkipTicks) and
   *  says so in the list header (bgRefreshFailing).
   *
   *  The failure half reads the BANNER rather than a flag of its own (nothing else ever sets
   *  traceqlError), so the reason on screen and the reason the list is frozen cannot drift
   *  apart: whatever clears the banner is exactly what thaws the list. */
  private readonly listHeld = computed(() => this.streamStopped() || this.traceqlError() !== '');
  /** Rows one stream will deliver before the server stops — sent as `?max=` (which the server
   *  clamps to 1…5000 and enforces by ending the stream), and the number the header names.
   *
   *  It bounds the ARRAY, not the DOM. A trace row is 13 elements before its second service
   *  chip, so 2000 of them rendered flat is ~26k — a bill the old 200-row page cap never sent
   *  the browser. What bounds the DOM is the virtualizer below: at a 600px viewport and ~82px
   *  a row, a 2000-row answer stands at 16 rendered rows / 209 elements behind a spacer 2000
   *  rows tall (measured, traces.background-intent.spec.ts). That is also what makes the 15 s
   *  refresh affordable: re-streaming 2000 rows re-diffs 2000 array entries but touches ~200
   *  elements. Raising this costs memory and wire time, not paint. */
  readonly streamMax = 2000;
  /** Consecutive failures of the BACKGROUND refresh (the 15 s poll). A user-initiated search
   *  never lands here — it gets the banner instead. Zeroed by any complete stream and by any
   *  search the user runs, so it only ever counts an unbroken run. */
  private readonly bgFailures = signal(0);
  /** Poll ticks still to be skipped before the next background refresh is attempted. Grows
   *  1 → 2 → 4 → 8 with the failure count (15 s → 2 min): re-firing a doomed query every
   *  15 s for as long as the page is open is not a retry policy. */
  private bgSkipTicks = 0;
  /** When the list last took delivery of a COMPLETE answer. Read only by bgRefreshHint(), to
   *  date the rows on screen when the refresh behind them has stopped working. */
  private readonly listLoadedAt = signal<number | null>(null);
  /** A background refresh has failed three times running (~45 s), so the list is no longer
   *  live and the user is owed that fact. Three, not one: a single dropped tick is noise.
   *  Suppressed while the list is held, where the header is already explaining itself. */
  readonly bgRefreshFailing = computed(() => this.bgFailures() >= 3 && !this.listHeld());
  /** Rows buffered before a new array is pushed into `traces`. One array per frame would
   *  re-render the whole list a thousand times over a full stream; one per 25 keeps the
   *  count visibly moving without the churn (the Events store does the same at 10). */
  private static readonly FlushEvery = 25;

  /** Refresh immediately when the tab is re-shown after being hidden. */
  private _onVisibility = () => { if (!document.hidden) this.poll(); };

  // ── Computed ──────────────────────────────────────────────────────────────
  services = computed(() => {
    const set = new Set(this.traces().map(t => t.serviceName).filter(Boolean));
    return [...set].sort();
  });

  filteredTraces = computed(() => this.traces());

  /** The list spinner may only show when there is no list. The spinner and the rows are the
   *  two arms of one @if, so letting it win while rows are on screen UNMOUNTS the scroll
   *  container — and the rows come back in a brand-new element at scrollTop 0. */
  readonly listLoading = computed(() => this.loading() && this.traces().length === 0);

  // ── Virtual list ──────────────────────────────────────────────────────────
  // `.trace-rows` is the scroll element; it only exists in the rows arm of the list's @if, so
  // both queries are legitimately empty while the spinner or the empty state is showing and
  // the virtualizer takes `undefined` for its scroll element until the arm mounts.
  private readonly traceScroll = viewChild<ElementRef<HTMLElement>>('traceScroll');
  private readonly traceRowEls = viewChildren<ElementRef<HTMLElement>>('traceRowEl');

  /**
   * Renders only the rows in view. @tanstack/angular-virtual is already a dependency and
   * already drives the Events list, so this is the house pattern rather than a new one.
   *
   * MEASURED, not fixed-height, which is where it differs from Events: a trace row is a
   * timestamp, a path that wraps to a second line when the method badge crowds it, and a
   * service-chip row that wraps once per ~3 services — in a panel that itself narrows from
   * 400px to 320px when a trace is open. Events could pin 29px because its rows are one line
   * by construction; pinning a height here would mean clipping the service chips, and hiding
   * which services a trace touched to save a measurement is not a trade worth making on this
   * page. `estimateSize` is the common case (one line each) and every rendered row corrects
   * itself; the correction is a no-op once measured, since resizeItem ignores a zero delta.
   */
  readonly rowVirtualizer = injectVirtualizer(() => ({
    count:         this.filteredTraces().length,
    scrollElement: this.traceScroll(),
    estimateSize:  () => 82,
    overscan:      8,
    getItemKey:    (i: number) => this.filteredTraces()[i]?.traceId ?? i,
  }));

  constructor() {
    // Drops the row elements the virtualizer still holds by key but that are no longer in the
    // document — what measureElement(null) is for. Keyed on traceId, those entries survive the
    // answer that produced them, so without this a page left open across many searches keeps a
    // detached node alive for every trace it has ever rendered. Tracks the ROW ARRAY, not the
    // rendered elements, so scrolling does not pay for a sweep on every frame.
    afterRenderEffect(() => {
      this.filteredTraces();
      this.rowVirtualizer.measureElement(null);
    });
    // Hands each rendered row to the virtualizer to be measured and observed. afterRender, not
    // effect: the elements have to exist and be laid out before their height means anything.
    // measureElement is idempotent per node — it re-observes only when the element behind a
    // key changed, and notifies only when the height actually moved — so this cannot spin.
    afterRenderEffect(() => {
      for (const el of this.traceRowEls()) this.rowVirtualizer.measureElement(el.nativeElement);
    });
  }

  /** Services offered in the filter — backend list ∪ services seen in loaded traces. */
  serviceOptions = computed(() => {
    const set = new Set<string>(this.allServices());
    for (const s of this.services()) set.add(s);
    return [...set].sort();
  });

  // ── Waterfall computed helpers ────────────────────────────────────────────
  private traceRange = computed<{ minNs: number; totalNs: number }>(() => {
    const spans = this.traceSpans();
    if (!spans.length) return { minNs: 0, totalNs: 1 };
    let minNs = spans[0].startTimeUnixNano;
    let maxEnd = spans[0].startTimeUnixNano + spans[0].durationNanos;
    for (const s of spans) {
      if (s.startTimeUnixNano < minNs) minNs = s.startTimeUnixNano;
      const end = s.startTimeUnixNano + s.durationNanos;
      if (end > maxEnd) maxEnd = end;
    }
    return { minNs, totalNs: Math.max(1, maxEnd - minNs) };
  });

  spanDepthMap = computed(() => {
    const spans = this.traceSpans();
    const byId  = new Map(spans.map(s => [s.spanId, s]));
    const map   = new Map<string, number>();
    for (const span of spans) {
      let depth = 0, cur: SpanDto | undefined = span;
      while (cur?.parentSpanId && !isZeroId(cur.parentSpanId)) {
        cur = byId.get(cur.parentSpanId);
        if (++depth > 20) break;
      }
      map.set(span.spanId, depth);
    }
    return map;
  });

  uniqueServices = computed(() => {
    const set = new Set(this.traceSpans().map(s => s.serviceName));
    return [...set];
  });

  selectedSpanTags = computed(() => {
    const span = this.selectedSpan();
    if (!span?.attributes) return [];
    return Object.entries(span.attributes).map(([key, value]) => ({ key, value }));
  });

  // ── Lifecycle ─────────────────────────────────────────────────────────────
  ngOnInit() {
    this.restoreFromUrl();
    this.setWindow();
    this.loadAllServices();
    this.loadAll();
    this._poll = setInterval(() => this.poll(), 15_000);
    document.addEventListener('visibilitychange', this._onVisibility);
  }

  /** Periodic live refresh. Skips work when the tab is hidden, and only restarts the
   *  (heavier) trace list while the Traces tab is actually on screen. */
  private poll() {
    if (document.hidden) return;
    const from = this.fromIso();
    const to   = this.toIso();
    this.loadStats(from, to);
    // Stats always refresh; the list does not while a stream is still delivering into it —
    // restarting mid-stream would throw away the rows already on screen and re-run the same
    // query from the top every 15 s, a list that never finishes arriving — and not while the
    // list is HELD, i.e. the user stopped it or it failed. 'background' is what makes the rest
    // of this tick invisible: nobody asked, so it may not raise a banner, may not offer Stop,
    // and — when there are rows to protect — buffers and swaps once so nothing the user is
    // reading moves under them.
    if (this.activeMainTab !== 'traces' || this.streaming() || this.listHeld()) return;
    // Back off after a background refresh fails. A held list stops polling outright because
    // the user owns it; a failing background refresh keeps trying, because the failure may be
    // a passing one (a spent budget on a busy server) and nobody asked for the list to go
    // stale — but it tries at a widening interval instead of hammering the same query.
    if (this.bgSkipTicks > 0) { this.bgSkipTicks--; return; }
    this.loadTraces(from, to, 'background');
  }

  private loadAllServices() {
    this.api.getServiceNames(30).subscribe({
      next: s => { this.allServices.set(s); this.cdr.markForCheck(); },
      error: () => { /* keep empty — falls back to services seen in loaded traces */ },
    });
  }

  // ── URL state sync (survives F5 / deep-link) ──────────────────────────────
  private restoreFromUrl() {
    const q = this.route.snapshot.queryParamMap;

    const tab = q.get('tab');
    if (tab === 'graph' || tab === 'latency' || tab === 'compare' || tab === 'traces')
      this.activeMainTab = tab;

    const range = q.get('range');
    if (range) this.preset = range;
    if (this.preset === 'custom') {
      this.customFrom = q.get('cfrom') ?? '';
      this.customTo   = q.get('cto')   ?? '';
    }

    const dtab = q.get('dtab');
    if (dtab === 'flamegraph' || dtab === 'details' || dtab === 'timeline')
      this.activeTraceTab = dtab;

    this.filterService    = q.get('svc')    ?? '';
    this.filterName       = q.get('name')   ?? '';
    this.filterStatus     = q.get('status') ?? '';
    this.filterHttpStatus = q.get('http')   ?? '';
    const min = q.get('min'); this.filterMinDurationMs = min ? +min : null;
    const max = q.get('max'); this.filterMaxDurationMs = max ? +max : null;

    const ql = q.get('ql');
    if (ql) { this.traceqlInput = ql; this.traceqlMode.set(true); }

    const trace = q.get('trace');
    if (trace) this.openTrace(trace);
  }

  /** Reflect the current page state into the URL query string (replaceUrl — no history spam). */
  private syncUrl() {
    const qp: Record<string, string | null> = {
      tab:    this.activeMainTab === 'traces'    ? null : this.activeMainTab,
      trace:  this.selectedTraceId() ?? null,
      dtab:   this.activeTraceTab === 'timeline' ? null : this.activeTraceTab,
      range:  this.preset === '1h'               ? null : this.preset,
      cfrom:  this.preset === 'custom' && this.customFrom ? this.customFrom : null,
      cto:    this.preset === 'custom' && this.customTo   ? this.customTo   : null,
      svc:    this.filterService    || null,
      name:   this.filterName       || null,
      status: this.filterStatus     || null,
      http:   this.filterHttpStatus || null,
      min:    this.filterMinDurationMs != null ? String(this.filterMinDurationMs) : null,
      max:    this.filterMaxDurationMs != null ? String(this.filterMaxDurationMs) : null,
      ql:     this.traceqlMode() && this.traceqlInput.trim() ? this.traceqlInput.trim() : null,
    };
    this.router.navigate([], {
      relativeTo:          this.route,
      queryParams:         qp,
      queryParamsHandling: 'merge',
      replaceUrl:          true,
    });
  }

  // ── State setters that also persist to URL ────────────────────────────────
  setMainTab(tab: 'traces' | 'graph' | 'latency' | 'compare') {
    this.activeMainTab = tab;
    this.syncUrl();
    // Leaving the list cancels its stream: nobody is watching the rows land, and an open
    // EventSource holds a server connection open for a panel that is off screen.
    if (tab !== 'traces') { this.stopStream(); return; }
    // Returning refreshes it — the poll leaves the list alone while it is off screen — unless
    // the list is held. Coming back to a tab is navigation, not a new search: the rows a Stop
    // (or a failure) left behind are still the answer the user was reading, and Refresh sits
    // right next to the header that says the list is partial. 'background' for the same
    // reason: the panel is only class-hidden, never unmounted, so the list is still scrolled
    // where they left it and a glance at Service Graph must not cost them that place — nor
    // hand them a banner about a query they did not run, if the re-run happens to fail.
    if (!this.listHeld()) this.loadTraces(this.fromIso(), this.toIso(), 'background');
  }

  setTraceTab(tab: 'timeline' | 'flamegraph' | 'details') {
    this.activeTraceTab = tab;
    this.syncUrl();
  }

  setPreset(p: string) {
    this.preset = p;
    this.syncUrl();
    // 'custom' just reveals the date inputs; the query runs on Apply.
    if (p !== 'custom') { this.setWindow(); this.loadAll(); }
  }

  /** Apply a custom range. `from` is required; `to` optional (empty → open-ended / now). */
  applyCustom() {
    if (!this.customFrom) return;
    this.syncUrl();
    this.setWindow();
    this.loadAll();
  }

  setTraceqlMode(on: boolean) {
    this.traceqlMode.set(on);
    this.traceqlError.set('');
    this.syncUrl();
  }

  toggleHistory(): void {
    this.historyOpen.update(v => !v);
  }

  /**
   * A click in the History panel — lands exactly like a hand-typed TraceQL search:
   * switch to TraceQL mode, drop the query in, clear any stale error, then run it
   * through the same user-run path (runTraceQLUser records + syncs the URL). The panel
   * itself stays open, same as the Events Signals panel.
   */
  // applyTraceql already does everything a history apply needs — including landing on the
  // Traces tab so the results render where the user can see them; the attrMenu close it
  // also performs is a harmless no-op from the panel.
  applyHistoryQuery(query: string): void {
    this.applyTraceql(query);
  }

  ngOnDestroy() {
    // Unsubscribed directly rather than through stopStream(): the view is going away, so
    // there is nothing left to flush into it and nothing left to mark for check.
    this.streamSub?.unsubscribe();
    this.streamSub = null;
    this.flushPending = null;
    this.streamPaints = false;
    if (this._poll) clearInterval(this._poll);
    document.removeEventListener('visibilitychange', this._onVisibility);
  }

  // ── Data loading ──────────────────────────────────────────────────────────
  /** Every caller is a user action — Refresh, Apply, Clear, a range chip, Apply-custom, a
   *  seek from the property menu — plus the initial load. That is exactly what releases a
   *  held list, and it must happen even when loadTraces declines to run (another tab is
   *  showing): the release is what lets the list refresh once the user comes back to it. */
  loadAll() {
    const from = this.fromIso();
    const to   = this.toIso();
    this.releaseList();
    this.loadStats(from, to);
    this.loadTraces(from, to);
  }

  loadStats(from: string, to?: string) {
    this.statsLoading.set(true);
    this.api.getTraceStats(from, to).subscribe({
      next: s  => { this.stats.set(s); this.statsLoading.set(false); this.cdr.markForCheck(); },
      error: () => { this.statsLoading.set(false); this.cdr.markForCheck(); },
    });
  }

  /**
   * Opens the list's stream — but only while the list is the panel on screen. The range
   * chips, the filter bar and the TraceQL box all render ABOVE the tab strip, so they stay
   * live while Service Graph / Latency / Compare is showing, and without this guard a chip
   * click there (or a deep link like /traces?tab=graph&ql=…) opened a 2000-row EventSource
   * into a hidden panel — the very connection setMainTab cancels on the way out. The stats
   * above the tabs still refresh; only the list waits, and setMainTab loads it on return.
   */
  loadTraces(from: string, to?: string, origin: LoadOrigin = 'user') {
    if (this.activeMainTab !== 'traces') return;
    if (this.traceqlMode() && this.traceqlInput.trim()) {
      this.runTraceQL(origin);
      return;
    }
    this.startStream(this.api.streamTraceList({
      from,
      to,
      service:        this.filterService        || undefined,
      spanName:       this.filterName           || undefined,
      status:         this.filterStatus         || undefined,
      minDurationMs:  this.filterMinDurationMs  ?? undefined,
      maxDurationMs:  this.filterMaxDurationMs  ?? undefined,
      httpStatus:     this.filterHttpStatus     || undefined,
      max:            this.streamMax,
    }), origin);
  }

  /** Reached two ways: the user pressing Run (through runTraceQLUser) and the 15 s poll,
   *  when a TraceQL query is the thing the list is currently showing — hence `origin`, which
   *  the poll's tick must carry all the way down or its failures become the user's. */
  runTraceQL(origin: LoadOrigin = 'user') {
    const q = this.traceqlInput.trim();
    if (!q) return;
    this.syncUrl();
    // The custom range's END, which this path used to drop on the floor: only `from` was
    // ever sent, so the server fell back to its "to = now" default and a query over, say,
    // last week silently ran to the present. Invisible whenever the end IS now — which is
    // why it survived — and wrong for every other custom window. The filter-bar path has
    // always sent it; TraceQL simply never did.
    this.startStream(this.api.streamTraceQuery({
      query: q, from: this.fromIso(), to: this.toIso(), max: this.streamMax,
    }), origin);
  }

  /**
   * The one way a search takes over the list. Cancelling the previous stream FIRST is what
   * makes "one search owns the list" true: the old EventSource is closed before the new one
   * is even asked for, so an abandoned search has no live callback that could interleave its
   * rows with the search that replaced it.
   *
   * Whether the list is emptied here depends on WHO asked, and the two answers are opposite.
   * A background refresh must not empty it: clearing synchronously destroyed the scroll
   * container — spinner in, rows out — so the 15 s poll handed the user a fresh empty element
   * at scrollTop 0 twice a minute, whatever they were reading. A search the USER ran must
   * empty it, for the mirror-image reason: its rows answer a different question, and leaving
   * them up means the header counts and describes the PREVIOUS result while the new one
   * streams — change service=A to service=B, press Apply, and "342 traces" sits over 342 rows
   * of service A until the first B row lands, a window bounded only by the stream deadline.
   *
   * @param origin  Who asked (see {@link LoadOrigin}) — and ONLY who asked. A 'background'
   *   refresh of a list that already has rows buffers them silently and swaps in ONE array
   *   when the stream completes: paint a partial batch and the list shrinks to 1 row before
   *   growing back, and a scroll container whose content collapses is clamped to scrollTop 0
   *   by the browser — the same lost place, reached the other way. Cancelled half way it
   *   publishes NOTHING, because a half-arrived refresh is not a better answer than the
   *   complete list already on screen. A user's own search pays nothing for progressive
   *   paint: they asked for different rows, and the top is where they want to be — so it
   *   keeps flushing per batch, and so does a background refresh that finds the list EMPTY,
   *   where there is no position left to protect. That last case is why `origin` exists as a
   *   parameter rather than being inferred from the row count: an empty list makes a poll
   *   tick paint like a search, and it must still not fail like one.
   */
  private startStream(source: Observable<TraceRowDto>, origin: LoadOrigin = 'user'): void {
    this.stopStream();
    // The stream about to open supersedes whatever the cancelled one left behind — including
    // the "stopped early" stopStream() just stamped on it, and the banner of the query before.
    this.releaseList();
    this.loading.set(true);
    this.streaming.set(true);

    // Two questions, two answers, and they are allowed to disagree.
    //
    // May I OWN THE FAILURE? — decided by who asked, and by nothing else. The row count is a
    // fact about the screen; it cannot make an unattended tick into a search the user ran.
    const userAsked = origin === 'user';
    // May I PAINT AS I GO? — decided by what is on screen, which is what the row count was
    // always for. Only a refresh that finds rows must hold them back; an empty list has no
    // scroll position to protect and nothing to lose, so the tick fills it as rows arrive.
    const silent = !userAsked && this.traces().length > 0;
    // The list exactly as this stream found it. A background refresh that fails puts it back
    // (see the error branch): for `silent` that is the array it never touched, and for the
    // empty case it is the empty list, so the prefix a dying tick painted is not left behind
    // to read as a finished answer that nothing on screen would call partial.
    const before = this.traces();

    // Stop and the "streaming…" hint are for a search the user is watching arrive. An
    // unattended tick has nothing they need to interrupt, whatever the list looked like when
    // it started.
    this.streamPublishing.set(userAsked);
    this.streamPaints = !silent;
    // Everything below belongs to a stream the USER asked for; a background refresh touches
    // none of it, which is what makes it invisible.
    if (userAsked) {
      // The old answer goes out with the question that produced it (see the doc comment).
      if (this.traces().length) this.traces.set([]);
      // `streamCapped` describes the list on screen, so a background refresh leaves the hint
      // standing until it completes with an answer of its own — otherwise "newest 2000 ·
      // narrow the range" blinked out and back twice a minute under a list that never moved.
      // The one background stream that DOES replace the list — the empty-list case — cannot
      // want this either: a capped list is 2000 rows, so it is never the empty one.
      this.streamCapped.set(false);
      // The user is driving again, so the background refresh starts from a clean slate: its
      // failure count and its backoff both describe a run of unattended ticks, and this is
      // not one. Whatever the poll could not fetch, this query is about to fetch itself.
      this.bgFailures.set(0);
      this.bgSkipTicks = 0;
    }

    // Rows land in a plain array and reach the signal in batches. Pushing a new array per
    // frame would re-render the entire list once per row — 2000 renders for one search.
    //
    // `flush` is captured in a local and the frame handler calls THAT, not the field: the
    // accumulator and the publisher that empties it are then one object, paired by closure
    // rather than by the unsubscribe-before-subscribe discipline holding. It holds today; if
    // a future edit ever starts a stream without stopping the previous one, a frame from the
    // old stream reading the field would publish the NEW stream's buffer.
    const acc: TraceRowDto[] = [];
    const flush = () => {
      this.traces.set([...acc]);
      this.loading.set(false);
    };
    // The field means "what this stream may hand over if it ends right now" — null for a
    // silent refresh, which has nothing worth handing over until it is complete.
    this.flushPending = silent ? null : flush;

    this.streamSub = source.subscribe({
      next: row => {
        acc.push(row);
        // The FIRST row flushes immediately so the empty-state spinner is replaced the
        // moment there is anything to show; after that, one flush per batch.
        if (!silent && (acc.length === 1 || acc.length % TracesComponent.FlushEvery === 0)) {
          flush();
          this.cdr.markForCheck();
        }
      },
      complete: () => {
        // Complete: a silent refresh now HAS the whole answer, so it hands it over the same
        // way a publishing stream hands over its tail — through endStream, in one array.
        this.flushPending = flush;
        // A stream that delivered exactly its ceiling was cut off, not finished: older
        // traces exist that the list does not contain, and the header has to say so.
        this.streamCapped.set(acc.length >= this.streamMax);
        // The list holds a whole answer as of now, and the refresh behind it is working.
        this.listLoadedAt.set(Date.now());
        this.bgFailures.set(0);
        this.bgSkipTicks = 0;
        this.endStream();
      },
      error: (err: unknown) => {
        // Two different failures land here: a TraceQL parse error, which arrives before any
        // row (as a query-error FRAME — that is why the server does not answer it with a 400
        // the browser could not read), and a query that dies halfway through on a spent time
        // budget or an unreadable segment.
        if (!userAsked) {
          // A background refresh that fails leaves the visible state EXACTLY as it found it,
          // and that is a promise about BOTH halves of what it could have touched. Its rows
          // are withheld: the buffered ones were never published, and the ones it painted into
          // an empty list go back to `before` — the complete answer on screen, even when that
          // answer was "none", beats the half one this tick managed to fetch. The banner is
          // withheld too: the user never ran this query, the rows under it are not partial,
          // and the banner is half of listHeld, so planting it would also freeze the list with
          // nothing on screen saying so. What they do learn is that the list stopped being
          // live — said in the list header, next to the count, once the failure has repeated
          // (see bgRefreshFailing), and phrased as the age of the rows rather than a claim
          // about them.
          this.flushPending = null;
          this.traces.set(before);
          this.bgFailures.update(n => n + 1);
          this.bgSkipTicks = Math.min(8, 2 ** (this.bgFailures() - 1));
        } else {
          // A search the user ran keeps whatever arrived and gets the reason beside it — the
          // half-answer plus its explanation, not an empty list and a banner. The banner is
          // also what holds the list (see listHeld) and what the header reads to say the
          // search ended in an error rather than a plain count.
          this.traceqlError.set((err as Error)?.message?.trim() || 'Query error');
        }
        this.endStream();
      },
    });
  }

  /**
   * Stops the active stream and keeps every row it already delivered. Reached from the
   * header's Stop button, from leaving the Traces tab, and from the start of the next search.
   */
  stopStream(): void {
    // Only a stream that was PAINTING leaves a partial list behind, and only then may the
    // header call it partial: called with nothing open (a second Stop, leaving the tab after
    // the stream finished) or over a silent refresh that never touched the rows, this must
    // not stamp "stopped early" on a list that is complete. `streamPaints`, not
    // `streamPublishing`: a background refresh of an EMPTY list does put rows on screen while
    // offering no Stop, and cancelling it half way still leaves a prefix that has to say so.
    if (this.streamSub && this.streamPaints) this.streamStopped.set(true);
    this.streamSub?.unsubscribe();
    this.endStream();
  }

  /** Settles the list after a stream ends, however it ended: the partial final batch is
   *  flushed here so the last rows are never stranded in the buffer. */
  private endStream(): void {
    this.flushPending?.();
    this.flushPending = null;
    this.streamSub    = null;
    this.streamPaints = false;
    this.streaming.set(false);
    this.streamPublishing.set(false);
    this.loading.set(false);
    this.cdr.markForCheck();
  }

  /** Hands the list back to the poll: whatever held it — a Stop, a failed stream and the
   *  banner it left — the user has now asked for something new, and the something new is what
   *  the list should show. Clearing the banner here is not cosmetic: it IS half the hold. */
  private releaseList(): void {
    this.streamStopped.set(false);
    this.traceqlError.set('');
  }

  /** The TraceQL box's ✕: drops the query, the results it produced, and the `?ql=` in the
   *  URL — which the old version left behind, so F5 resurrected the query just cleared. */
  clearTraceql(): void {
    this.traceqlInput = '';
    this.stopStream();
    this.traces.set([]);
    this.streamCapped.set(false);
    // Nothing on screen for a stale "rows from 14:32" to be about, and the next tick is a
    // fresh start rather than the continuation of a failing run.
    this.listLoadedAt.set(null);
    this.bgFailures.set(0);
    this.bgSkipTicks = 0;
    // An emptied list is nobody's to keep: the next tick may refill it (with no query, the
    // filter-bar path), which is what clearing the box asks for. Also drops the banner.
    this.releaseList();
    this.syncUrl();
  }

  /**
   * The only two user-initiated ways to run a TraceQL query (Run button, Enter in the
   * box) — wired here instead of inside runTraceQL() itself, which is ALSO the 15 s
   * live-poll path (loadTraces → runTraceQL when a query is already active) and the
   * ?ql= deep-link auto-run path (restoreFromUrl → openTrace doesn't touch it, but the
   * initial loadAll does). Recording inside runTraceQL() would re-record on every poll
   * tick and every cross-page jump that lands here with ?ql= set.
   */
  runTraceQLUser(): void {
    if (!this.traceqlInput.trim()) return;
    this.history.record(this.traceqlInput);
    // The box renders above the tab strip, so Run can be pressed with Service Graph on
    // screen — and loadTraces refuses to stream into a hidden list, which would make the
    // button look dead. Take the user to the results instead; runTraceQL's own syncUrl()
    // then writes the tab it just landed on.
    this.activeMainTab = 'traces';
    // A search the user asked for outranks whatever was holding the list.
    this.releaseList();
    this.runTraceQL();
  }

  applyFilters() {
    const q = this.synthesizeTraceql();
    if (q) this.history.record(q);
    // Same reason Run switches tabs: the filter bar renders above the tab strip, so Apply is
    // clickable with Service Graph on screen, and loadTraces refuses to stream into a hidden
    // list. A search the user ran has to land where its results are.
    this.activeMainTab = 'traces';
    this.syncUrl();
    this.loadAll();
  }

  /** The filter bar's Clear: blank every field and re-run. Deliberately NOT applyFilters() —
   *  a reset is not a request to see results, so it does not drag the user off the panel they
   *  are on. (Also why it is a method: this was eight assignments in a template expression.) */
  clearFilters() {
    this.filterName          = '';
    this.filterService       = '';
    this.filterStatus        = '';
    this.filterMinDurationMs = null;
    this.filterMaxDurationMs = null;
    this.filterHttpStatus    = '';
    this.syncUrl();
    this.loadAll();
  }

  /**
   * Builds the TraceQL predicate the populated filter-bar fields would mean, so a
   * filter-mode Apply can be recorded in the same TraceQL vocabulary the history panel
   * (and the manual TraceQL box) already speaks — same grammar tqlPredicate below
   * targets (TraceQLParser.BuildIntrinsicPredicate / BuildAttrPredicate). Field order
   * follows the bar's own left-to-right layout; empty fields are skipped, and '' comes
   * back when nothing is set — a plain Clear (which blanks every field before calling
   * applyFilters) records nothing.
   */
  private synthesizeTraceql(): string {
    // Two fields have no honest TraceQL form, and their treatment is ALL-or-nothing: the
    // span-name box matches by case-insensitive SUBSTRING server-side while the grammar's
    // `=` is exact, and the HTTP bucket picks ('2xx'/'4xx'/'5xx') have no single-value
    // form. Dropping just the offending field from a COMBINED search would record a
    // strictly WIDER query than the one that ran — replayed, it silently returns more
    // than the user saw. A search touching either field is therefore not recorded at
    // all: no entry beats an entry that lies about what it will find, in either direction.
    if (this.filterName) return '';
    if (this.filterHttpStatus && !/^d+$/.test(this.filterHttpStatus)) return '';

    const parts: string[] = [];
    if (this.filterService) parts.push(this.tqlPredicate('service', this.filterService, false));
    if (this.filterStatus)  parts.push(`status = ${this.filterStatus.toLowerCase()}`);
    if (this.filterMinDurationMs != null) parts.push(`duration >= ${this.filterMinDurationMs}ms`);
    if (this.filterMaxDurationMs != null) parts.push(`duration <= ${this.filterMaxDurationMs}ms`);
    if (this.filterHttpStatus) parts.push(this.tqlPredicate('.http.status_code', this.filterHttpStatus, false));
    return parts.length ? `{ ${parts.join(' && ')} }` : '';
  }

  toggleTraceQL() {
    this.traceqlMode.update(v => !v);
    this.traceqlError.set('');
    this.syncUrl();
  }

  jumpToTrace() {
    const id = this.traceIdInput.trim();
    if (!id) return;
    this.openTrace(id);
  }

  openTrace(traceId: string) {
    if (this.selectedTraceId() === traceId) return;
    this.selectedTraceId.set(traceId);
    this.selectedSpan.set(null);
    this.resetLogs();
    this.syncUrl();
    this.traceLoading.set(true);
    this.api.getTrace(traceId).subscribe({
      next: spans => {
        this.traceSpans.set(spans.sort((a, b) => a.startTimeUnixNano - b.startTimeUnixNano));
        this.traceLoading.set(false);
        this.cdr.markForCheck();
      },
      error: () => { this.traceLoading.set(false); this.cdr.markForCheck(); },
    });
  }

  closeTrace() {
    this.selectedTraceId.set(null);
    this.traceSpans.set([]);
    this.selectedSpan.set(null);
    this.resetLogs();
    this.syncUrl();
  }

  /** Brief ✓ feedback after the trace id is copied from the detail header. */
  readonly traceIdCopied = signal(false);

  async copyTraceId(): Promise<void> {
    const id = this.selectedTraceId();
    if (!id) return;
    await this.copyText(id);
    this.traceIdCopied.set(true);
    setTimeout(() => { this.traceIdCopied.set(false); this.cdr.markForCheck(); }, 1500);
  }

  private async copyText(text: string): Promise<void> {
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      // navigator.clipboard needs a secure context (https/localhost) and focus —
      // plain-http hosts (e.g. http://sandbox:8555) get the legacy fallback.
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      ta.remove();
    }
  }

  // ── Span property context menu (Tags table + the left-column fields) ───────

  readonly attrMenu = signal<{
    key: string;
    value: string;
    /** TraceQL left-hand side (e.g. '.env', 'service.name', 'name'); null = copy-only. */
    tqlKey: string | null;
    /** CLEF key for the "Find in logs" cross; null hides the cross item. */
    logKey: string | null;
    /** Epoch ms when the value is a date/timestamp — enables the Seek section. */
    seekMs: number | null;
    x: number;
    y: number;
  } | null>(null);

  /** Menu for an arbitrary span attribute (Tags table): searchable + logs cross. */
  openAttrMenu(ev: MouseEvent, key: string, value: unknown): void {
    const s = String(value);
    this.openMenuAt(ev, { key, value: s, tqlKey: `.${key}`, logKey: key, seekMs: parseDateMs(s) });
  }

  /**
   * Menu for a left-column span field. `tqlKey` null = copy-only (e.g. Span ID,
   * Parent, Duration); `logKey` null = no logs cross; `seekMs` set for timestamps.
   */
  openFieldMenu(ev: MouseEvent, key: string, value: unknown, tqlKey: string | null, logKey: string | null, seekMs: number | null = null): void {
    this.openMenuAt(ev, { key, value: String(value), tqlKey, logKey, seekMs });
  }

  private openMenuAt(ev: MouseEvent, m: { key: string; value: string; tqlKey: string | null; logKey: string | null; seekMs: number | null }): void {
    ev.stopPropagation();
    const x = Math.min(ev.clientX, window.innerWidth - 240);
    const y = Math.min(ev.clientY, window.innerHeight - 290);
    this.attrMenu.set({ ...m, x, y });
  }

  /** Seek the time range to the field's timestamp ± N seconds. */
  attrSeek(seconds: number): void {
    const m = this.attrMenu();
    if (m?.seekMs == null) return;
    this.attrMenu.set(null);
    this.preset = 'custom';
    this.customFrom = msToLocal(m.seekMs - seconds * 1000);
    this.customTo   = msToLocal(m.seekMs + seconds * 1000);
    this.syncUrl();
    this.setWindow();
    this.loadAll();
  }

  @HostListener('document:click')
  closeAttrMenu(): void {
    if (this.attrMenu()) this.attrMenu.set(null);
  }

  async attrCopy(what: 'value' | 'key'): Promise<void> {
    const m = this.attrMenu();
    if (!m) return;
    await this.copyText(what === 'value' ? m.value : m.key);
    this.attrMenu.set(null);
  }

  /** Replace the query: search traces by this field alone. */
  attrFind(neq: boolean): void {
    const m = this.attrMenu();
    if (!m?.tqlKey) return;
    this.applyTraceql(`{ ${this.tqlPredicate(m.tqlKey, m.value, neq)} }`);
  }

  /** Append to the current query with && / ||. */
  attrExpr(joiner: '&&' | '||', neq: boolean): void {
    const m = this.attrMenu();
    if (!m?.tqlKey) return;
    const pred = this.tqlPredicate(m.tqlKey, m.value, neq);
    let inner = '';
    if (this.traceqlMode()) {
      const q = this.traceqlInput.trim();
      const braced = q.match(/^\{([\s\S]*)\}$/);
      inner = (braced ? braced[1] : q).trim();
    }
    this.applyTraceql(inner ? `{ (${inner}) ${joiner} ${pred} }` : `{ ${pred} }`);
  }

  /** Cross-signal jump: open the Logs page filtered by the same property. */
  attrFindInLogs(): void {
    const m = this.attrMenu();
    if (!m?.logKey) return;
    this.attrMenu.set(null);
    const ident = /^[A-Za-z_][A-Za-z0-9_]*$/.test(m.logKey) ? m.logKey : `['${m.logKey}']`;
    const value = /^-?\d+(\.\d+)?$/.test(m.value) ? m.value : `'${m.value.replaceAll("'", "''")}'`;
    void this.router.navigate(['/events'], { queryParams: { filter: `${ident} = ${value}` } });
  }

  /** A log row on the Logs tab emitted a CLEF filter — open it on the Events page. */
  findInLogs(filter: string): void {
    void this.router.navigate(['/events'], { queryParams: { filter } });
  }

  /** `tqlKey` is the already-formatted TraceQL LHS (intrinsic without a dot, attribute with). */
  private tqlPredicate(tqlKey: string, value: string, neq: boolean): string {
    const op = neq ? '!=' : '=';
    const v  = /^-?\d+(\.\d+)?$/.test(value) || value === 'true' || value === 'false'
      ? value
      : `"${value.replaceAll('"', '')}"`;
    return `${tqlKey} ${op} ${v}`;
  }

  private applyTraceql(query: string): void {
    this.attrMenu.set(null);
    this.traceqlMode.set(true);
    this.traceqlError.set('');
    this.traceqlInput = query;
    this.activeMainTab = 'traces';
    this.runTraceQLUser();
  }

  selectSpan(span: SpanDto) {
    const same = this.selectedSpan()?.spanId === span.spanId;
    this.selectedSpan.set(same ? null : span);
    if (!same) this.activeSpanTab = 'tags';
  }

  /** Closes the span detail panel. */
  closeSpan(): void {
    this.selectedSpan.set(null);
  }

  /**
   * Escape closes the innermost layer: the log modal (whose own EventDetail owns Escape
   * and emits `closed` — this page listener is registered first, so it defers) → the
   * span detail panel.
   */
  @HostListener('document:keydown.escape')
  onEscape(): void {
    if (this.attrMenu()) { this.attrMenu.set(null); return; }
    if (this.historyOpen()) { this.historyOpen.set(false); return; }
    if (this.logModalEvent()) return;
    if (this.selectedSpan()) this.selectedSpan.set(null);
  }

  private resetLogs() {
    this.traceLogs.set([]);
    this.traceLogsLoaded.set(false);
    this.onlyThisSpan.set(false);
  }

  /** Opens the Logs tab and lazily loads all logs for the trace (once). */
  openSpanLogs() {
    this.activeSpanTab = 'logs';
    if (this.traceLogsLoaded() || this.traceLogsLoading()) return;

    const traceId = this.selectedTraceId();
    const spans   = this.traceSpans();
    if (!traceId || !spans.length) return;

    // Bound the query to the trace's own time span (+ buffer) so the executor
    // can skip far segments. Logs are written by the same processes that own
    // the spans, so their @t falls inside [traceStart, traceEnd].
    let minNs = spans[0].startTimeUnixNano;
    let maxNs = spans[0].startTimeUnixNano + spans[0].durationNanos;
    for (const s of spans) {
      if (s.startTimeUnixNano < minNs) minNs = s.startTimeUnixNano;
      const end = s.startTimeUnixNano + s.durationNanos;
      if (end > maxNs) maxNs = end;
    }
    const BUF_MS = 60_000;
    const from = new Date(minNs / 1_000_000 - BUF_MS).toISOString();
    const to   = new Date(maxNs / 1_000_000 + BUF_MS).toISOString();

    this.traceLogsLoading.set(true);
    this.api.getTraceLogs(traceId, from, to).subscribe({
      next: logs => {
        this.traceLogs.set(logs);
        this.traceLogsLoaded.set(true);
        this.traceLogsLoading.set(false);
        this.cdr.markForCheck();
      },
      error: () => { this.traceLogsLoading.set(false); this.cdr.markForCheck(); },
    });
  }

  logMatchesSelectedSpan(log: EventDto): boolean {
    const sp = this.selectedSpan()?.spanId;
    return !!sp && log['@sp'] === sp;
  }

  private readonly presetHours: Record<string, number> = {
    '15m': 0.25, '30m': 0.5, '1h': 1, '3h': 3, '6h': 6, '12h': 12, '24h': 24,
  };

  /** Query lower bound — custom value, or "now − preset" (fresh each call → live list). */
  private fromIso(): string {
    if (this.preset === 'custom') return localToIso(this.customFrom);
    return formatISO(subHours(new Date(), this.presetHours[this.preset] ?? 1));
  }

  /** Query upper bound — only in custom mode; presets stay open-ended (server "now"). */
  private toIso(): string | undefined {
    return this.preset === 'custom' ? (localToIso(this.customTo) || undefined) : undefined;
  }

  /** Freezes the current window into winFrom/winTo (drives Graph/Latency). User action only. */
  private setWindow(): void {
    this.winFrom.set(this.fromIso());
    this.winTo.set(this.toIso());
  }

  // ── Waterfall helpers ─────────────────────────────────────────────────────
  waterfallLeft(span: SpanDto): number {
    const { minNs, totalNs } = this.traceRange();
    return ((span.startTimeUnixNano - minNs) / totalNs) * 100;
  }

  waterfallWidth(span: SpanDto): number {
    const { totalNs } = this.traceRange();
    return Math.max(0.3, (span.durationNanos / totalNs) * 100);
  }

  spanDepth(span: SpanDto): number {
    return this.spanDepthMap().get(span.spanId) ?? 0;
  }

  wfTimeTicks(): { pct: number; label: string }[] {
    const { totalNs } = this.traceRange();
    const totalMs = totalNs / 1_000_000;
    return Array.from({ length: 5 }, (_, i) => ({
      pct:   (i / 4) * 100,
      label: fmtMs(totalMs * i / 4),
    }));
  }

  totalDurLabel(): string {
    const { totalNs } = this.traceRange();
    return fmtMs(totalNs / 1_000_000);
  }

  traceServicesLabel(trace: TraceRowDto): string[] {
    return trace.services?.length ? trace.services : [trace.serviceName].filter(Boolean);
  }

  /**
   * What a repeatedly failing background refresh is allowed to say. It names the thing that
   * actually broke — the live refresh — and dates the rows; it says nothing about whether the
   * rows are partial, because they are not: they are the last COMPLETE answer, which is
   * exactly why they were kept. The clock is the moment of that answer, so it does not tick.
   */
  bgRefreshHint(): string {
    const ms = this.listLoadedAt();
    return ms == null
      ? '· live refresh failing — this list is not updating'
      : `· live refresh failing — rows as of ${format(new Date(ms), 'HH:mm:ss')}`;
  }

  // ── Formatting ────────────────────────────────────────────────────────────
  fmtTraceTime(nanos: number): string {
    return format(new Date(Math.round(nanos / 1_000_000)), 'dd/MM/yyyy HH:mm:ss.SSS');
  }

  fmtDurNs(nanos: number): string {
    return fmtMs(nanos / 1_000_000);
  }

  statusCls(status: string): string {
    return status === 'Error' ? 'error' : 'ok';
  }

  statusBadgeLabel(code: number | null, status: string): string {
    if (code) return code >= 400 ? `${code} ERROR` : `${code} OK`;
    return status === 'Error' ? 'Error' : status === 'Ok' ? 'OK' : status;
  }

  statusBadgeCls(code: number | null, status: string): string {
    if (status === 'Error' || (code != null && code >= 500)) return 'badge-error';
    if (code != null && code >= 400) return 'badge-warn';
    return 'badge-ok';
  }

  /** Stable per-service colour (shared hash palette — consistent with Logs / Stats). */
  svcColor(name: string): string {
    return serviceColor(name);
  }

  sparkline(data: number[]): string {
    if (!data?.length || data.length < 2) return '';
    const max = Math.max(...data, 1);
    const W = 80, H = 28;
    return data.map((v, i) => `${(i / (data.length - 1)) * W},${H - (v / max) * H}`).join(' ');
  }

  fmtStat(v: number, unit: 'count' | 'pct' | 'ms' | 'rps'): string {
    if (unit === 'ms')  return v < 1 ? '<1ms' : v >= 1000 ? `${(v / 1000).toFixed(2)}s` : `${Math.round(v)}ms`;
    if (unit === 'pct') return `${v.toFixed(2)}%`;
    if (unit === 'rps') {
      if (v >= 1)      return `${v.toFixed(1)} rps`;
      const tpm = v * 60;
      if (tpm >= 1)    return `${tpm.toFixed(1)}/min`;
      const tph = v * 3600;
      if (tph >= 1)    return `${Math.round(tph)}/h`;
      return '<1/h';
    }
    if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
    if (v >= 1_000)     return `${(v / 1_000).toFixed(1)}K`;
    return String(Math.round(v));
  }

  logLevel(log: EventDto): string {
    return log['@l'] ?? 'Information';
  }

  logMessage(log: EventDto): string {
    return log['@mt'] ?? '';
  }

  logTs(log: EventDto): string {
    // `@t` is the ISO string the server sent; the old `typeof` fork existed only because the
    // model claimed it might be a Date.
    const t = log['@t'];
    return t ? t.substring(11, 23) : '';
  }

  logLevelCls(log: EventDto): string {
    const l = this.logLevel(log).toLowerCase();
    if (l === 'error' || l === 'fatal') return 'lvl-error';
    if (l === 'warning' || l === 'warn') return 'lvl-warn';
    if (l === 'debug' || l === 'verbose') return 'lvl-debug';
    return 'lvl-info';
  }
}

function isZeroId(id: string): boolean {
  return !id || /^0+$/.test(id);
}

/** datetime-local ("yyyy-MM-ddTHH:mm") → ISO-8601, or '' when empty/invalid. */
function localToIso(local: string): string {
  if (!local) return '';
  const d = new Date(local);
  return isNaN(d.getTime()) ? '' : d.toISOString();
}

/** Recognises an ISO-8601-ish date string and returns its epoch ms, else null. */
function parseDateMs(v: string): number | null {
  if (!/^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}/.test(v)) return null;
  const ms = Date.parse(v);
  return isNaN(ms) ? null : ms;
}

/** epoch ms → "yyyy-MM-ddTHH:mm:ss" in LOCAL time (datetime-local with seconds). */
function msToLocal(ms: number): string {
  const d = new Date(ms - new Date(ms).getTimezoneOffset() * 60_000);
  return d.toISOString().slice(0, 19);
}

function fmtMs(ms: number): string {
  if (ms < 0.001) return `${(ms * 1_000_000).toFixed(0)}ns`;
  if (ms < 1)     return `${(ms * 1_000).toFixed(0)}µs`;
  if (ms < 1_000) return `${ms.toFixed(2)}ms`;
  return `${(ms / 1_000).toFixed(3)}s`;
}

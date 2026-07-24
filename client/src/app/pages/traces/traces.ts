import {
  Component, signal, computed, inject, OnInit, OnDestroy, HostListener,
  ChangeDetectionStrategy, ChangeDetectorRef,
} from '@angular/core';
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
  imports: [FormsModule, LucideAngularModule, ServiceGraphComponent, FlamegraphComponent, LatencyComponent, CompareTraceComponent, SuggestInputDirective, ModalComponent, EventDetailComponent, EventListRowComponent, PropertyMenuComponent],
  templateUrl: './traces.html',
  styleUrl: './traces.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class TracesComponent implements OnInit, OnDestroy {
  private api    = inject(ApiService);
  private cdr    = inject(ChangeDetectorRef);
  private router = inject(Router);
  private route  = inject(ActivatedRoute);

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
  traceqlError   = signal('');
  /** Candidates for the TraceQL Ctrl+Space autocomplete. */
  readonly traceqlSuggestions = TRACEQL_TOKENS as string[];

  private _poll: ReturnType<typeof setInterval> | null = null;
  /** Refresh immediately when the tab is re-shown after being hidden. */
  private _onVisibility = () => { if (!document.hidden) this.poll(); };

  // ── Computed ──────────────────────────────────────────────────────────────
  services = computed(() => {
    const set = new Set(this.traces().map(t => t.serviceName).filter(Boolean));
    return [...set].sort();
  });

  filteredTraces = computed(() => this.traces());

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

  /** Periodic live refresh. Skips work when the tab is hidden, and only refetches the
   *  (heavier) trace list while the Traces tab is actually on screen. */
  private poll() {
    if (document.hidden) return;
    const from = this.fromIso();
    const to   = this.toIso();
    this.loadStats(from, to);
    if (this.activeMainTab === 'traces') this.loadTraces(from, to);
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
    // Returning to the list refreshes it (polling leaves it alone while off-screen).
    if (tab === 'traces') this.loadTraces(this.fromIso(), this.toIso());
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

  ngOnDestroy() {
    if (this._poll) clearInterval(this._poll);
    document.removeEventListener('visibilitychange', this._onVisibility);
  }

  // ── Data loading ──────────────────────────────────────────────────────────
  loadAll() {
    const from = this.fromIso();
    const to   = this.toIso();
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

  loadTraces(from: string, to?: string) {
    if (this.traceqlMode() && this.traceqlInput.trim()) {
      this.runTraceQL();
      return;
    }
    this.loading.set(true);
    this.api.searchTraces({
      from,
      to,
      service:        this.filterService        || undefined,
      spanName:       this.filterName           || undefined,
      status:         this.filterStatus         || undefined,
      minDurationMs:  this.filterMinDurationMs  ?? undefined,
      maxDurationMs:  this.filterMaxDurationMs  ?? undefined,
      httpStatus:     this.filterHttpStatus     || undefined,
      limit:          500,
    }).subscribe({
      next: rows => { this.traces.set(rows); this.loading.set(false); this.cdr.markForCheck(); },
      error: () =>  { this.loading.set(false); this.cdr.markForCheck(); },
    });
  }

  runTraceQL() {
    const q = this.traceqlInput.trim();
    if (!q) return;
    this.syncUrl();
    this.loading.set(true);
    this.traceqlError.set('');
    this.api.queryTraces({ query: q, from: this.fromIso(), limit: 200 }).subscribe({
      next: rows => { this.traces.set(rows); this.loading.set(false); this.cdr.markForCheck(); },
      error: (err) => {
        this.loading.set(false);
        this.traceqlError.set(err?.error?.message ?? 'Query error');
        this.cdr.markForCheck();
      },
    });
  }

  applyFilters() { this.syncUrl(); this.loadAll(); }

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
    x: number;
    y: number;
  } | null>(null);

  /** Menu for an arbitrary span attribute (Tags table): searchable + logs cross. */
  openAttrMenu(ev: MouseEvent, key: string, value: unknown): void {
    this.openMenuAt(ev, { key, value: String(value), tqlKey: `.${key}`, logKey: key });
  }

  /**
   * Menu for a left-column span field. `tqlKey` null = copy-only (e.g. Span ID,
   * Parent, Start, Duration); `logKey` null = no logs cross.
   */
  openFieldMenu(ev: MouseEvent, key: string, value: unknown, tqlKey: string | null, logKey: string | null): void {
    this.openMenuAt(ev, { key, value: String(value), tqlKey, logKey });
  }

  private openMenuAt(ev: MouseEvent, m: { key: string; value: string; tqlKey: string | null; logKey: string | null }): void {
    ev.stopPropagation();
    const x = Math.min(ev.clientX, window.innerWidth - 240);
    const y = Math.min(ev.clientY, window.innerHeight - 290);
    this.attrMenu.set({ ...m, x, y });
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
    this.runTraceQL();
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
    const t = log['@t'];
    if (!t) return '';
    const s = typeof t === 'string' ? t : t.toISOString();
    return s.substring(11, 23);
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

function fmtMs(ms: number): string {
  if (ms < 0.001) return `${(ms * 1_000_000).toFixed(0)}ns`;
  if (ms < 1)     return `${(ms * 1_000).toFixed(0)}µs`;
  if (ms < 1_000) return `${ms.toFixed(2)}ms`;
  return `${(ms / 1_000).toFixed(3)}s`;
}

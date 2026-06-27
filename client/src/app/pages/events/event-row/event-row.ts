import {
  Component, computed, signal, inject, HostListener,
  input, output, ChangeDetectionStrategy, effect, untracked,
  viewChild, TemplateRef, ViewContainerRef,
} from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { forkJoin, of } from 'rxjs';
import { switchMap, catchError, map } from 'rxjs/operators';
import { DomSanitizer } from '@angular/platform-browser';
import { NgTemplateOutlet, DatePipe } from '@angular/common';
import { Overlay, OverlayRef } from '@angular/cdk/overlay';
import { TemplatePortal } from '@angular/cdk/portal';
import { LucideAngularModule } from 'lucide-angular';

import { EventDto } from '../../../core/models/event.model';
import { SpanDto } from '../../../core/models/span.model';
import { MetricSeriesDto } from '../../../core/models/metric.model';
import { UserPreferencesService } from '../../../core/services/user-preferences.service';
import { ApiService } from '../../../core/services/api.service';
import { renderMessageHtml, escapeHtmlExport } from '../../../shared/utils/clef-renderer';
import { JsonViewerComponent } from '../../../shared/components/json-viewer/json-viewer';
import { MetricSparkComponent } from '../../../shared/components/metric-spark/metric-spark';
import {
  JsonViewerActions, JvMenuRequest, jvLiteral, jvWildcard,
} from '../../../shared/components/json-viewer/json-viewer.actions';

const LEVEL_SHORT: Record<string, string> = {
  verbose: 'VRB', debug: 'DBG', information: 'INF',
  warning: 'WRN', error: 'ERR', fatal: 'FTL',
};

export interface PropEntry {
  /** Full dot-path usable in filter expressions, e.g. "Payload.Customer.Id" */
  path: string;
  /** Property key, shown in the table */
  label: string;
  /** Stringified value for scalar props; empty for structured (object/array) props */
  value: string;
  /** Raw value — passed to the inline JSON viewer when {@link isStructured} is true */
  raw: unknown;
  /** True when the value is a non-empty object/array (rendered as an expandable JSON viewer) */
  isStructured: boolean;
}

function formatInline(v: unknown): string {
  if (v === null || v === undefined) return 'null';
  if (Array.isArray(v))               return JSON.stringify(v);
  if (typeof v === 'object')          return JSON.stringify(v);
  return String(v);
}

/** Safe identifier as accepted by the server-side lexer (letter/digit/_/@). */
const IDENT_RE = /^[A-Za-z_@][A-Za-z0-9_@]*$/;

/**
 * Append a child segment to a property path, using bracket notation when the
 * segment contains characters the filter lexer cannot consume as an identifier
 * (hyphens, spaces, slashes, etc.).
 *   joinPath('Headers', 'Foo')               → 'Headers.Foo'
 *   joinPath('Headers', 'Api-Request-Id')    → "Headers['Api-Request-Id']"
 *   joinPath('',        'My-Top')            → "['My-Top']"
 */
function joinPath(prefix: string, key: string): string {
  if (IDENT_RE.test(key))
    return prefix ? `${prefix}.${key}` : key;
  const escaped = key.replace(/'/g, "\\'");
  return `${prefix}['${escaped}']`;
}

/** Top-level keys promoted to dedicated rows in the detail panel — skip in the generic props list. */
const PROMOTED_KEYS = new Set(['@tr', '@sp', 'service.name']);

/**
 * Builds the flat top-level property list. Scalar values become text rows;
 * non-empty objects/arrays become a single structured row rendered by the
 * inline JSON viewer (Seq/Datalust-style — collapsed to one line, expandable).
 * Nesting is handled by the viewer itself, so values are no longer flattened
 * into many indented rows.
 */
function buildProps(obj: Record<string, unknown>): PropEntry[] {
  const out: PropEntry[] = [];
  for (const [k, v] of Object.entries(obj)) {
    if (PROMOTED_KEYS.has(k)) continue;
    const structured = v !== null && typeof v === 'object' && Object.keys(v as object).length > 0;
    out.push({
      path: joinPath('', k),
      label: k,
      value: structured ? '' : formatInline(v),
      raw: v,
      isStructured: structured,
    });
  }
  return out;
}

function wfFmtMs(ms: number): string {
  if (ms < 0.001) return `${(ms * 1_000_000).toFixed(0)}ns`;
  if (ms < 1)     return `${(ms * 1_000).toFixed(0)}µs`;
  if (ms < 1_000) return `${ms.toFixed(2)}ms`;
  return `${(ms / 1_000).toFixed(3)}s`;
}

@Component({
  selector: 'app-event-row',
  imports: [LucideAngularModule, NgTemplateOutlet, DatePipe, JsonViewerComponent, MetricSparkComponent],
  providers: [DatePipe, JsonViewerActions],
  templateUrl: './event-row.html',
  styleUrl: './event-row.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
  host: {
    '[class.ev-verbose]':     'levelKey() === "verbose"',
    '[class.ev-debug]':       'levelKey() === "debug"',
    '[class.ev-information]': 'levelKey() === "information"',
    '[class.ev-warning]':     'levelKey() === "warning"',
    '[class.ev-error]':       'levelKey() === "error"',
    '[class.ev-fatal]':       'levelKey() === "fatal"',
    '[class.ev-expanded]':    'expanded()',
    '[class.ev-wrap-mode]':   'wrap()',
  },
})
export class EventRowComponent {
  // ── Inputs / outputs ──────────────────────────────────────────────────
  event = input.required<EventDto>();
  wrap  = input<boolean>(false);
  expandedEventIds = input<ReadonlySet<string>>(new Set());
  filterSelected = output<string>();
  seekRequested  = output<{ from: Date; to: Date }>();
  expandRequested = output<string>();

  private api = inject(ApiService);

  // ── Local state ───────────────────────────────────────────────────────
  expanded  = computed(() => this.expandedEventIds().has(this.event().id));
  messageExpanded = signal(false);
  detailTab  = signal<'overview' | 'message' | 'json' | 'exception' | 'trace' | 'metrics'>('overview');
  jsonSearch = signal('');
  menuType  = signal<'message' | 'level' | 'export' | 'jv' | null>(null);
  menuPos   = signal({ x: 0, y: 0 });
  /** Active JSON-viewer node context (path/value/kind) when {@link menuType} is 'jv'. */
  jvMenu = signal<JvMenuRequest | null>(null);

  // ── Trace waterfall state ─────────────────────────────────────────────
  readonly traceSpans        = signal<SpanDto[]>([]);
  readonly traceSpansLoading = signal(false);
  readonly selectedWfSpan    = signal<SpanDto | null>(null);
  readonly lastFetchedTid    = signal('');
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private traceSubRef?: any;

  /** Key of the copy button showing the "copied" confirmation, null otherwise. */
  copiedKey = signal<string | null>(null);
  private copiedTimer?: ReturnType<typeof setTimeout>;

  // ── Correlated metrics state ──────────────────────────────────────────
  readonly metricSeries      = signal<{ name: string; series: MetricSeriesDto[] }[]>([]);
  readonly metricsLoading    = signal(false);
  private lastFetchedMetricId = '';
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private metricsSubRef?: any;

  private sanitizer = inject(DomSanitizer);
  private datePipe  = inject(DatePipe);
  private prefs = inject(UserPreferencesService);
  private overlay = inject(Overlay);
  private vcr = inject(ViewContainerRef);
  private jvActions = inject(JsonViewerActions);
  private overlayRef: OverlayRef | null = null;
  ctxMenuTpl = viewChild<TemplateRef<unknown>>('ctxMenuTpl');

  constructor() {
    // A filter icon inside the inline JSON viewer was clicked → open the
    // context menu (CDK overlay) at the click position with that node's path.
    this.jvActions.menu.pipe(takeUntilDestroyed()).subscribe(req => {
      this.jvMenu.set(req);
      let x = req.x;
      let y = req.y + 6;
      if (x + 240 > window.innerWidth)  x = req.x - 240;
      if (y + 200 > window.innerHeight) y = req.y - 200;
      this.menuPos.set({ x, y });
      this.menuType.set('jv');
    });

    // CDK virtual scroll recycles row instances across different events.
    // Reset local UI state whenever the bound event changes so an expanded /
    // menu-open state from a previous row does not leak onto a new one.
    effect(() => {
      this.event(); // track input identity
      untracked(() => {
        this.menuType.set(null);
        this.jvMenu.set(null);
        this.messageExpanded.set(false);
        this.detailTab.set('overview');
        this.lastFetchedTid.set('');
        this.traceSpans.set([]);
        this.traceSpansLoading.set(false);
        this.selectedWfSpan.set(null);
        this.lastFetchedMetricId = '';
        this.metricSeries.set([]);
        this.metricsLoading.set(false);
      });
    });

    // Fetch trace spans lazily when the Trace tab is first opened.
    effect(() => {
      const tab = this.detailTab();
      const tid = this.traceId();
      if (tab !== 'trace' || !tid) return;
      if (this.lastFetchedTid() === tid) return;

      untracked(() => {
        this.traceSubRef?.unsubscribe();
        this.lastFetchedTid.set(tid);
        this.traceSpansLoading.set(true);
        this.traceSpans.set([]);
        this.selectedWfSpan.set(null);

        this.traceSubRef = this.api.getTrace(tid).subscribe({
          next: spans => {
            this.traceSpans.set([...spans].sort((a, b) => a.startTimeUnixNano - b.startTimeUnixNano));
            this.traceSpansLoading.set(false);
          },
          error: () => this.traceSpansLoading.set(false),
        });
      });
    });

    // Fetch correlated metrics when the Metrics tab is first opened.
    effect(() => {
      const tab     = this.detailTab();
      const eventId = this.event().id;
      if (tab !== 'metrics') return;
      if (this.lastFetchedMetricId === eventId) return;

      const numericKeys = Object.entries(this.event().props ?? {})
        .filter(([, v]) => typeof v === 'number')
        .map(([k]) => k);

      untracked(() => {
        this.metricsSubRef?.unsubscribe();
        this.lastFetchedMetricId = eventId;
        this.metricsLoading.set(true);
        this.metricSeries.set([]);

        if (!numericKeys.length) { this.metricsLoading.set(false); return; }

        const ts   = new Date(this.event()['@t'] as unknown as string).getTime();
        const from = new Date(ts - 10 * 60_000).toISOString();
        const to   = new Date(ts + 10 * 60_000).toISOString();

        this.metricsSubRef = this.api.getMetricNames().pipe(
          switchMap(names => {
            const matching = numericKeys.filter(k => names.includes(k));
            if (!matching.length) return of([] as { name: string; series: MetricSeriesDto[] }[]);
            return forkJoin(
              Object.fromEntries(
                matching.map(k => [k, this.api.queryMetric(k, from, to).pipe(catchError(() => of([])))])
              )
            ).pipe(
              map(res => Object.entries(res).map(([name, series]) => ({ name, series: series as MetricSeriesDto[] })))
            );
          }),
          catchError(() => of([] as { name: string; series: MetricSeriesDto[] }[])),
        ).subscribe({
          next:  entries => { this.metricSeries.set(entries); this.metricsLoading.set(false); },
          error: ()       => this.metricsLoading.set(false),
        });
      });
    });

    // Render the context menu through a CDK overlay so it escapes the
    // transformed virtual-scroll container (where `position: fixed` would
    // otherwise be positioned relative to the transformed ancestor).
    effect(() => {
      const type = this.menuType();
      const pos  = this.menuPos();
      const tpl  = this.ctxMenuTpl();
      untracked(() => this.syncOverlay(type, pos, tpl));
    });
  }

  private syncOverlay(
    type: 'message' | 'level' | 'export' | 'jv' | null,
    pos: { x: number; y: number },
    tpl: TemplateRef<unknown> | undefined,
  ): void {
    if (!type || !tpl) {
      this.overlayRef?.detach();
      return;
    }
    const positionStrategy = this.overlay.position()
      .global()
      .left(`${pos.x}px`)
      .top(`${pos.y}px`);
    if (!this.overlayRef) {
      this.overlayRef = this.overlay.create({
        positionStrategy,
        scrollStrategy: this.overlay.scrollStrategies.reposition(),
        panelClass: 'ctx-menu-overlay',
      });
    } else {
      this.overlayRef.updatePositionStrategy(positionStrategy);
    }
    if (!this.overlayRef.hasAttached()) {
      this.overlayRef.attach(new TemplatePortal(tpl, this.vcr));
    }
  }

  ngOnDestroy(): void {
    clearTimeout(this.copiedTimer);
    this.traceSubRef?.unsubscribe();
    this.metricsSubRef?.unsubscribe();
    this.overlayRef?.dispose();
    this.overlayRef = null;
  }

  eventTsMs = computed(() =>
    new Date(this.event()['@t'] as unknown as string).getTime()
  );

  // ── Derived ───────────────────────────────────────────────────────────
  levelKey = computed(() => (this.event()['@l'] ?? 'information').toLowerCase());

  levelShort = computed(() => LEVEL_SHORT[this.levelKey()] ?? this.levelKey().slice(0, 3).toUpperCase());

  service = computed(() =>
    (this.event()['service.name'] as string | undefined) ?? ''
  );

  traceId = computed(() =>
    (this.event()['@tr'] as string | undefined) ??
    (this.event().props?.['@tr'] as string | undefined) ??
    ''
  );

  spanId = computed(() =>
    (this.event()['@sp'] as string | undefined) ??
    (this.event().props?.['@sp'] as string | undefined) ??
    ''
  );

  // timestamp = computed(() => {
  //   const raw = this.event()['@t'];
  //   try {
  //     const d = new Date(raw);
  //     const datePart = this.datePipe.transform(d, 'MM-dd HH:mm:ss') ?? raw;
  //     const fracMatch = raw.match(/\.(\d+)/);
  //     const ms = fracMatch ? fracMatch[1].slice(0, 3).padEnd(3, '0') : '000';
  //     return `${datePart}.${ms}`;
  //   } catch { return raw; }
  // });

  renderedHtml = computed(() =>
    this.sanitizer.bypassSecurityTrustHtml(
      renderMessageHtml(this.event()['@mt'], this.event().props)
    )
  );

  rawJson = computed(() => JSON.stringify(this.event(), null, 2));

  /**
   * CLEF-style flat view of the event: system fields + user props at the same
   * level, matching how the server filter language addresses them.
   * Used by the JSON tab so filter-path expressions are correct (e.g.
   * `Headers.Authorization`, not `props.Headers.Authorization`).
   */
  clefView = computed<Record<string, unknown>>(() => {
    const ev = this.event();
    const view: Record<string, unknown> = {};
    if (ev['@t']           !== undefined) view['@t']           = ev['@t'];
    if (ev['@l']           !== undefined) view['@l']           = ev['@l'];
    if (ev['@mt']          !== undefined) view['@mt']          = ev['@mt'];
    if (ev['@x']           !== undefined) view['@x']           = ev['@x'];
    if (ev['@tr']          !== undefined) view['@tr']          = ev['@tr'];
    if (ev['@sp']          !== undefined) view['@sp']          = ev['@sp'];
    if (ev['service.name'] !== undefined) view['service.name'] = ev['service.name'];
    Object.assign(view, ev.props ?? {});
    return view;
  });

  topProps = computed(() =>
    Object.entries(this.event().props ?? {})
      .slice(0, 3)
      .map(([k, v]) => ({ k, v: formatInline(v) }))
  );

  /**
   * Flat top-level property list:
   *  - Scalar values  → text row { path:'A', label:'A', value:'42', isStructured:false }
   *  - Object/array   → structured row rendered inline by the JSON viewer
   *                     (collapsed to one line, expandable — Seq/Datalust-style)
   */
  allProps = computed(() => buildProps(this.event().props ?? {}));

  propsCount   = computed(() => Object.keys(this.event().props ?? {}).length);
  hasException  = computed(() => !!this.event()['@x']);

  overviewCustomItems = computed(() => {
    const props = this.event().props ?? {};
    return this.prefs.overviewCustomProps()
      .map(k => ({ key: k, value: props[k] }))
      .filter(it => it.value !== undefined && it.value !== null && String(it.value).length > 0)
      .map(it => ({ key: it.key, value: formatInline(it.value) }));
  });

  metricItems = computed(() => Object.entries(this.event().props ?? {})
    .filter(([, v]) => typeof v === 'number')
    .map(([k, v]) => ({ key: k, value: Number(v).toLocaleString() }))
  );

  traceAttrs = computed(() => Object.entries(this.event().props ?? {})
    .filter(([k]) =>
      k.startsWith('trace.') ||
      k.startsWith('span.') ||
      k.startsWith('otel.') ||
      k === 'trace_id' ||
      k === 'span_id')
    .map(([k, v]) => ({ key: k, value: formatInline(v) }))
  );

  parentSpanId = computed(() => {
    const p = this.event().props ?? {};
    return (p['parentSpanId'] as string | undefined) ??
      (p['ParentSpanId'] as string | undefined) ??
      (p['parent_span_id'] as string | undefined) ??
      '';
  });

  /** Duration of this span in milliseconds, null if not available. */
  traceDuration = computed<number | null>(() => {
    const p = this.event().props ?? {};
    for (const k of ['durationMs', 'DurationMs', 'duration', 'Duration', 'elapsedMs', 'ElapsedMs', 'elapsed', 'Elapsed']) {
      const v = p[k];
      if (typeof v === 'number' && Number.isFinite(v)) return v;
      if (typeof v === 'string') { const n = parseFloat(v); if (!isNaN(n)) return n; }
    }
    return null;
  });

  traceDurationLabel = computed(() => {
    const ms = this.traceDuration();
    if (ms === null) return '';
    if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`;
    if (ms >= 0.1) return `${Math.round(ms)} ms`;
    return `${(ms * 1000).toFixed(0)} µs`;
  });

  traceparent = computed(() => {
    const tid = this.traceId().replace(/-/g, '');
    const sid = this.spanId().replace(/-/g, '');
    if (!tid || !sid) return '';
    return `00-${tid.padStart(32, '0')}-${sid.padStart(16, '0')}-01`;
  });

  httpInfo = computed(() => {
    const p = this.event().props ?? {};
    const method = String(p['http.method'] ?? p['http.request.method'] ?? '');
    const status = String(p['http.status_code'] ?? p['http.response.status_code'] ?? p['statusCode'] ?? '');
    const url = String(p['http.url'] ?? p['http.target'] ?? p['http.route'] ?? '');
    return { method, status, url };
  });

  /** All OTel-convention attributes: http.*, db.*, span.*, otel.*, trace.*, rpc.*, messaging.*, plus common trace IDs. */
  otelAttrs = computed(() => {
    const prefixes = ['http.', 'db.', 'span.', 'otel.', 'trace.', 'rpc.', 'messaging.', 'enduser.', 'net.', 'peer.', 'exception.'];
    const exact = new Set(['trace_id', 'span_id', 'parent_span_id', 'parentSpanId', 'ParentSpanId']);
    return Object.entries(this.event().props ?? {})
      .filter(([k]) => prefixes.some(p => k.startsWith(p)) || exact.has(k))
      .map(([k, v]) => ({ key: k, raw: v, value: formatInline(v) }));
  });

  // ── Trace waterfall computed ──────────────────────────────────────────
  private wfRange = computed<{ minNs: number; totalNs: number }>(() => {
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

  private wfDepthMap = computed(() => {
    const spans = this.traceSpans();
    const byId  = new Map(spans.map(s => [s.spanId, s]));
    const map   = new Map<string, number>();
    for (const span of spans) {
      let depth = 0, cur: SpanDto | undefined = span;
      while (cur?.parentSpanId && !/^0+$/.test(cur.parentSpanId)) {
        cur = byId.get(cur.parentSpanId);
        if (++depth > 20) break;
      }
      map.set(span.spanId, depth);
    }
    return map;
  });

  wfTicks = computed(() => {
    const { totalNs } = this.wfRange();
    const totalMs = totalNs / 1_000_000;
    return Array.from({ length: 5 }, (_, i) => ({
      pct:   (i / 4) * 100,
      label: wfFmtMs(totalMs * i / 4),
    }));
  });

  selectedWfSpanTags = computed(() => {
    const span = this.selectedWfSpan();
    if (!span?.attributes) return [];
    return Object.entries(span.attributes).map(([k, v]) => ({ k, v }));
  });

  wfLeft(span: SpanDto): number {
    const { minNs, totalNs } = this.wfRange();
    return ((span.startTimeUnixNano - minNs) / totalNs) * 100;
  }

  wfWidth(span: SpanDto): number {
    const { totalNs } = this.wfRange();
    return Math.max(0.3, (span.durationNanos / totalNs) * 100);
  }

  wfDepth(span: SpanDto): number {
    return this.wfDepthMap().get(span.spanId) ?? 0;
  }

  wfSvcColor(name: string): string {
    const PALETTE = ['#38BDF8', '#F59E0B', '#22C55E', '#a78bfa', '#f97316', '#ec4899', '#06b6d4', '#84cc16'];
    let h = 0;
    for (const c of name) h = (h * 31 + c.charCodeAt(0)) & 0x7fff_ffff;
    return PALETTE[h % PALETTE.length];
  }

  wfFmt(nanos: number): string { return wfFmtMs(nanos / 1_000_000); }

  selectWfSpan(span: SpanDto): void {
    this.selectedWfSpan.update(s => s?.spanId === span.spanId ? null : span);
  }

  labelChips = computed(() => {
    const p = this.event().props ?? {};
    const chips: Array<{ k: string; v: string }> = [];
    const keys = ['version', 'instance', 'region', 'host', 'environment', 'env'];
    for (const k of keys) {
      const v = p[k];
      if (v !== null && v !== undefined && String(v).length > 0) {
        chips.push({ k, v: String(v) });
      }
    }
    return chips;
  });

  durationLabel = computed(() => {
    const p = this.event().props ?? {};
    const candidates = ['durationMs', 'duration', 'elapsedMs', 'Elapsed', 'elapsed'];
    for (const k of candidates) {
      const v = p[k];
      if (typeof v === 'number' && Number.isFinite(v)) return `${v}ms`;
      if (typeof v === 'string' && v.trim().length > 0) return v;
    }
    return 'n/a';
  });

  // ── Interaction ───────────────────────────────────────────────────────
  toggleExpand(e: Event): void {
    e.stopPropagation();
    if (this.menuType()) { this.menuType.set(null); return; }
    const opening = !this.expanded();
    this.expandRequested.emit(this.event().id);
    if (opening && this.hasException()) this.detailTab.set('exception');
    else if (opening)                   this.detailTab.set('overview');
    if (opening) this.jsonSearch.set('');
  }

  // ── JSON-viewer context menu ──────────────────────────────────────────
  /** `path = value` (leaf) — exact match. */
  jvEqExpr = computed(() => {
    const m = this.jvMenu();
    return m ? `${m.path} = ${jvLiteral(m.rawValue)}` : '';
  });

  /** `path <> value` (leaf) — exclude. */
  jvNeExpr = computed(() => {
    const m = this.jvMenu();
    return m ? `${m.path} <> ${jvLiteral(m.rawValue)}` : '';
  });

  /** `items[%].id = value` — match the leaf in any array element (null if no index). */
  jvAnyExpr = computed(() => {
    const m = this.jvMenu();
    if (!m) return null;
    const wild = jvWildcard(m.path);
    return wild ? `${wild} = ${jvLiteral(m.rawValue)}` : null;
  });

  private emitFilter(expr: string): void {
    this.filterSelected.emit(expr);
    this.menuType.set(null);
  }

  jvFind(): void {
    const m = this.jvMenu();
    if (!m) return;
    this.emitFilter(m.isContainer ? `has(${m.path})` : this.jvEqExpr());
  }

  jvExclude(): void {
    const m = this.jvMenu();
    if (!m) return;
    this.emitFilter(m.isContainer ? `not has(${m.path})` : this.jvNeExpr());
  }

  jvFindAny(): void {
    const expr = this.jvAnyExpr();
    if (expr) this.emitFilter(expr);
  }

  async jvCopy(): Promise<void> {
    const m = this.jvMenu();
    if (!m) return;
    const text = m.isContainer ? m.path : jvLiteral(m.rawValue);
    await navigator.clipboard.writeText(text);
    this.menuType.set(null);
  }

  openMenu(e: MouseEvent, type: 'message' | 'level' | 'export'): void {
    e.stopPropagation();
    e.preventDefault();
    let x = e.clientX;
    let y = e.clientY + 6;
    if (x + 210 > window.innerWidth)  x = e.clientX - 210;
    if (y + 260 > window.innerHeight) y = e.clientY - 260;
    this.menuType.set(type);
    this.menuPos.set({ x, y });
  }

  @HostListener('document:click')
  @HostListener('document:keydown')
  onCloseMenu(): void {
    if (this.menuType()) this.menuType.set(null);
  }

  // ── Context menu actions ──────────────────────────────────────────────
  findSimilar(): void {
    const tmpl = this.event()['@mt'] ?? '';
    this.filterSelected.emit(`@mt = '${tmpl.replace(/'/g, "''")}'`);
    this.menuType.set(null);
  }

  findFrom(): void {
    this.seekRequested.emit({ from: new Date(this.event()['@t']), to: new Date() });
    this.menuType.set(null);
  }

  findTo(): void {
    this.seekRequested.emit({ from: new Date(0), to: new Date(this.event()['@t']) });
    this.menuType.set(null);
  }

  seek(seconds: number): void {
    const ms = new Date(this.event()['@t']).getTime();
    this.seekRequested.emit({
      from: new Date(ms - seconds * 1000),
      to:   new Date(ms + seconds * 1000),
    });
    this.menuType.set(null);
  }

  filterLevel(): void {
    this.filterSelected.emit(`@l = '${this.event()['@l']}'`);
    this.menuType.set(null);
  }

  excludeLevel(): void {
    this.filterSelected.emit(`@l <> '${this.event()['@l']}'`);
    this.menuType.set(null);
  }

  async copyMessage(): Promise<void> {
    const tmp = document.createElement('div');
    tmp.innerHTML = this.renderedHtml() as string;
    await navigator.clipboard.writeText(tmp.textContent ?? '');
    this.menuType.set(null);
    this.flashCopied('message');
  }

  async copyText(text: string, key?: string): Promise<void> {
    await navigator.clipboard.writeText(text);
    this.flashCopied(key ?? text);
  }

  private flashCopied(key: string): void {
    clearTimeout(this.copiedTimer);
    this.copiedKey.set(key);
    this.copiedTimer = setTimeout(() => this.copiedKey.set(null), 1800);
  }

  filterAttr(key: string, raw: unknown): void {
    this.filterSelected.emit(`${key} = ${jvLiteral(raw)}`);
  }

  async copyRaw(): Promise<void> {
    await navigator.clipboard.writeText(JSON.stringify(this.event()));
    this.menuType.set(null);
  }

  copyLink(): void {
    const url = `${window.location.origin}/events?id=${encodeURIComponent(this.event().id ?? '')}`;
    navigator.clipboard.writeText(url);
    this.menuType.set(null);
  }

  protected readonly escapeHtmlExport = escapeHtmlExport;
}

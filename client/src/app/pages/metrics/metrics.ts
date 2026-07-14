import {
  Component, signal, computed, effect, inject, viewChild, ElementRef,
  ChangeDetectionStrategy, ChangeDetectorRef, OnInit, OnDestroy,
} from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import type { Chart as ChartJs } from 'chart.js';
import { loadChart } from '../../shared/utils/chart-lazy';
import { format, subHours, formatISO } from 'date-fns';
import { forkJoin, of } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { ApiService } from '../../core/services/api.service';
import {
  MetricSeriesDto, MetricCatalogDto, MetricQueryRequest, MetricAggregation,
  HeatmapDto, ExemplarDto, MetricExprOp,
} from '../../core/models/metric.model';
import { TraceRowDto } from '../../core/models/span.model';
import { serviceColor } from '../../shared/utils/service-color';
import { HeatmapComponent } from './heatmap/heatmap';

const PRESETS: readonly [string, number][] = [
  ['15m', 0.25], ['30m', 0.5], ['1h', 1], ['3h', 3], ['6h', 6], ['12h', 12], ['24h', 24],
];
const AGGS: MetricAggregation[] = ['rate', 'increase', 'avg', 'min', 'max', 'last', 'sum', 'quantile'];
const OPS: MetricExprOp[] = ['div', 'mul', 'add', 'sub'];
const NICE_STEPS_SEC = [5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 10800, 21600];
const PALETTE = ['#38BDF8', '#F59E0B', '#22C55E', '#a78bfa', '#f97316', '#ec4899', '#06b6d4', '#84cc16', '#eab308', '#f43f5e'];

interface ExprSide { metric: string; agg: MetricAggregation; filters: string; }

/**
 * Metrics — a Grafana-Explore-style single view. The catalog picker drives a query
 * builder (aggregation / quantile / group-by / filters / top-K); the step is derived
 * from the window so the backend only ever returns ~1 point per few pixels (bounded
 * cost, matching the rollup-based storage).
 *
 * Extras for histograms: a bucket heatmap (cell → correlated traces), exemplar dots
 * (dot → the trace that produced it), and an A-op-B expression builder (ratios).
 */
@Component({
  selector: 'app-metrics',
  imports: [FormsModule, LucideAngularModule, HeatmapComponent],
  templateUrl: './metrics.html',
  styleUrl: './metrics.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class MetricsComponent implements OnInit, OnDestroy {
  private api    = inject(ApiService);
  private cdr    = inject(ChangeDetectorRef);
  private router = inject(Router);
  private route  = inject(ActivatedRoute);

  private readonly chartCanvas = viewChild<ElementRef<HTMLCanvasElement>>('chartCanvas');
  private chart: ChartJs | null = null;
  /** Monotonic guard: only the newest async draw is allowed to create a chart. */
  private drawSeq = 0;
  private _poll: ReturnType<typeof setInterval> | null = null;
  private _onVisibility = () => { if (!document.hidden) this.runQuery(); };

  readonly presets = PRESETS.map(p => p[0]);
  readonly aggs    = AGGS;
  readonly ops     = OPS;

  // ── State ─────────────────────────────────────────────────────────────────
  catalog     = signal<MetricCatalogDto[]>([]);
  search      = signal('');
  selected    = signal<MetricCatalogDto | null>(null);
  preset      = signal('1h');
  customFrom  = '';
  customTo    = '';
  rangeSec    = signal(3600);
  aggregation = signal<MetricAggregation>('rate');
  quantile    = signal(0.95);
  groupBy     = signal<string[]>([]);
  filtersRaw  = signal('');
  topk        = signal(12);
  series      = signal<MetricSeriesDto[]>([]);
  loading     = signal(false);

  // Modes / views
  mode          = signal<'query' | 'expr'>('query');
  viewMode      = signal<'lines' | 'heatmap'>('lines');
  showExemplars = signal(true);
  heatmap       = signal<HeatmapDto | null>(null);
  exemplarCount = signal(0);
  private exemplars: ExemplarDto[] = [];

  // Expression (A op B)
  exprLeft:  ExprSide = { metric: '', agg: 'rate', filters: '' };
  exprRight: ExprSide = { metric: '', agg: 'rate', filters: '' };
  exprOp    = signal<MetricExprOp>('div');
  exprScale = signal(100);

  // Correlation drawer (heatmap cell → traces)
  corrOpen    = signal(false);
  corrLoading = signal(false);
  corrTitle   = signal('');
  corrSub     = signal('');
  corrTraces  = signal<TraceRowDto[]>([]);

  private pending: { agg: MetricAggregation; q: number; gb: string[]; filters: string } | null = null;

  filteredCatalog = computed(() => {
    const q = this.search().trim().toLowerCase();
    const c = this.catalog();
    return q ? c.filter(m => m.name.toLowerCase().includes(q)) : c;
  });
  metricNames  = computed(() => this.catalog().map(m => m.name));
  isHistogram  = computed(() => this.selected()?.type === 'Histogram');
  scale        = computed(() => this.isHistogram() && this.selected()?.unit === 's' ? 1000 : 1);

  /** Auto step: aim for ~200 points across the window, snapped to a nice value. */
  step    = computed(() => niceStepSec(this.rangeSec()));
  stepStr = computed(() => fmtStep(this.step()));

  constructor() {
    // Redraw the line chart whenever data or the canvas element changes.
    effect(() => {
      const cv   = this.chartCanvas();
      const data = this.series();
      if (cv) void this.draw(cv.nativeElement, data);
    });
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────
  ngOnInit() {
    this.restoreFromUrl();
    this.loadCatalog();
    this._poll = setInterval(() => { if (!document.hidden) this.runQuery(); }, 30_000);
    document.addEventListener('visibilitychange', this._onVisibility);
  }

  ngOnDestroy() {
    this.chart?.destroy();
    if (this._poll) clearInterval(this._poll);
    document.removeEventListener('visibilitychange', this._onVisibility);
  }

  // ── URL state (deep-link / F5) ────────────────────────────────────────────
  private restoreFromUrl() {
    const q = this.route.snapshot.queryParamMap;
    const range = q.get('range');
    if (range) {
      this.preset.set(range);
      if (range === 'custom') { this.customFrom = q.get('cfrom') ?? ''; this.customTo = q.get('cto') ?? ''; }
      else this.rangeSec.set(this.hoursOf(range) * 3600);
    }
    if (q.get('metric')) {
      this.pending = {
        agg:     (q.get('agg') as MetricAggregation) || 'rate',
        q:       q.get('q') ? +q.get('q')! : 0.95,
        gb:      q.get('gb') ? q.get('gb')!.split(',').filter(Boolean) : [],
        filters: q.get('filters') ?? '',
      };
    }
  }

  private syncUrl() {
    const m = this.selected();
    const inQuery = this.mode() === 'query';
    const qp: Record<string, string | null> = {
      range:   this.preset() === '1h' ? null : this.preset(),
      cfrom:   this.preset() === 'custom' && this.customFrom ? this.customFrom : null,
      cto:     this.preset() === 'custom' && this.customTo   ? this.customTo   : null,
      metric:  inQuery && m ? m.name : null,
      agg:     inQuery && m ? this.aggregation() : null,
      q:       inQuery && m && this.aggregation() === 'quantile' ? String(this.quantile()) : null,
      gb:      inQuery && m && this.groupBy().length ? this.groupBy().join(',') : null,
      filters: inQuery && m && this.filtersRaw().trim() ? this.filtersRaw().trim() : null,
    };
    this.router.navigate([], {
      relativeTo: this.route, queryParams: qp, queryParamsHandling: 'merge', replaceUrl: true,
    });
  }

  // ── Catalog ───────────────────────────────────────────────────────────────
  private loadCatalog() {
    this.api.getMetricCatalog().pipe(catchError(() => of([] as MetricCatalogDto[]))).subscribe(c => {
      this.catalog.set(c);
      const wanted = this.route.snapshot.queryParamMap.get('metric');
      const now    = Date.now();
      const recent = (m: MetricCatalogDto) => !!m.lastSeenMs && now - m.lastSeenMs < 2 * 3600_000;
      const histByRecent = c.filter(m => m.type === 'Histogram')
                            .sort((a, b) => (b.lastSeenMs || 0) - (a.lastSeenMs || 0));
      const pick = (wanted && c.find(m => m.name === wanted))
        || histByRecent.find(m => /http\.server\.request\.duration/i.test(m.name) && recent(m))
        || histByRecent[0]
        || c[0];
      if (pick) {
        const p = this.pending;
        this.pending = null;
        this.selected.set(pick);
        this.aggregation.set(p?.agg ?? defaultAgg(pick));
        this.quantile.set(p?.q ?? 0.95);
        this.groupBy.set(p?.gb ?? []);
        this.filtersRaw.set(p?.filters ?? '');
        this.exprLeft.metric  ||= pick.name;
        this.exprRight.metric ||= pick.name;
        this.runQuery();
      }
      this.cdr.markForCheck();
    });
  }

  // ── Selection & controls ──────────────────────────────────────────────────
  selectMetric(m: MetricCatalogDto) {
    if (this.selected()?.name === m.name && this.mode() === 'query') return;
    this.mode.set('query');
    this.selected.set(m);
    this.groupBy.set([]);
    this.filtersRaw.set('');
    this.aggregation.set(defaultAgg(m));
    if (m.type !== 'Histogram') this.viewMode.set('lines');
    this.runQuery();
  }

  onAgg(a: MetricAggregation)  { this.aggregation.set(a); this.runQuery(); }
  onQuantile(q: number)        { this.quantile.set(+q);    this.runQuery(); }
  onTopk(n: number)            { this.topk.set(+n);        this.runQuery(); }
  toggleGroupBy(key: string) {
    this.groupBy.update(g => g.includes(key) ? g.filter(k => k !== key) : [...g, key]);
    this.runQuery();
  }

  setMode(m: 'query' | 'expr')       { this.mode.set(m); this.runQuery(); }
  setViewMode(v: 'lines' | 'heatmap') { this.viewMode.set(v); this.runQuery(); }
  toggleExemplars()                   { this.showExemplars.update(x => !x); this.runQuery(); }

  setPreset(p: string) {
    this.preset.set(p);
    if (p !== 'custom') { this.rangeSec.set(this.hoursOf(p) * 3600); this.syncUrl(); this.runQuery(); }
    else this.syncUrl();
  }
  applyCustom() {
    if (!this.customFrom) return;
    const fromMs = new Date(this.customFrom).getTime();
    const toMs   = this.customTo ? new Date(this.customTo).getTime() : Date.now();
    if (isNaN(fromMs)) return;
    this.rangeSec.set(Math.max(60, (toMs - fromMs) / 1000));
    this.syncUrl();
    this.runQuery();
  }

  resetZoom() { (this.chart as any)?.resetZoom?.(); }

  // ── Query ─────────────────────────────────────────────────────────────────
  runQuery() {
    if (this.mode() === 'expr') { this.runExpr(); return; }
    const m = this.selected();
    if (!m) return;
    this.loading.set(true);
    this.syncUrl();

    const from = this.fromIso(), to = this.toIso(), step = this.stepStr();
    const filters = this.parseFilters(this.filtersRaw());
    const histogram = m.type === 'Histogram';
    const noGroup   = this.groupBy().length === 0;

    // Heatmap: histogram + heatmap view + no group-by.
    if (histogram && this.viewMode() === 'heatmap' && noGroup) {
      this.api.getMetricHeatmap(m.name, from, to, step, filters)
        .pipe(catchError(() => of(null as unknown as HeatmapDto)))
        .subscribe(h => { this.heatmap.set(h); this.cdr.markForCheck(); });
    } else {
      this.heatmap.set(null);
    }

    // Exemplars: histogram + lines view + toggle on.
    const wantEx = histogram && this.showExemplars() && this.viewMode() === 'lines';
    const ex$ = wantEx
      ? this.api.getMetricExemplars(m.name, from, to, filters, 400).pipe(catchError(() => of([] as ExemplarDto[])))
      : of([] as ExemplarDto[]);

    const req: MetricQueryRequest = {
      metric:      m.name, from, to, step,
      aggregation: this.aggregation(),
      quantile:    this.aggregation() === 'quantile' ? this.quantile() : undefined,
      groupBy:     this.groupBy().length ? this.groupBy() : undefined,
      filters,
      topk:        this.topk(),
    };

    forkJoin({
      series:    this.api.queryMetricAgg(req).pipe(catchError(() => of([] as MetricSeriesDto[]))),
      exemplars: ex$,
    }).subscribe(r => {
      this.exemplars = r.exemplars;
      this.exemplarCount.set(r.exemplars.length);
      this.series.set(r.series);
      this.loading.set(false);
      this.cdr.markForCheck();
    });
  }

  /** Expression mode: evaluate left op right and graph the single result series. */
  runExpr() {
    this.heatmap.set(null);
    this.exemplars = [];
    this.exemplarCount.set(0);
    if (!this.exprLeft.metric || !this.exprRight.metric) { this.series.set([]); return; }
    this.loading.set(true);

    const from = this.fromIso(), to = this.toIso(), step = this.stepStr();
    const side = (s: ExprSide): MetricQueryRequest =>
      ({ metric: s.metric, from, to, step, aggregation: s.agg, filters: this.parseFilters(s.filters) });

    this.api.queryMetricExpr({
      left: side(this.exprLeft), right: side(this.exprRight),
      op: this.exprOp(), scale: this.exprScale(), name: 'expr',
    }).pipe(catchError(() => of(null as unknown as MetricSeriesDto))).subscribe(res => {
      this.series.set(res ? [res] : []);
      this.loading.set(false);
      this.cdr.markForCheck();
    });
  }

  // ── Correlation: heatmap cell → traces in that bucket ──────────────────────
  onHeatmapCell(e: { tsMs: number; loMs: number; hiMs: number; count: number }) {
    const half = this.step() * 1000; // ± one step, in ms
    const hiLabel = isFinite(e.hiMs) ? fmtMs(e.hiMs) : '∞';
    this.corrTitle.set(`Traces · ${fmtMs(e.loMs)}–${hiLabel}`);
    this.corrSub.set(`around ${format(new Date(e.tsMs), 'HH:mm:ss')} · ${e.count} samples`);
    this.corrOpen.set(true);
    this.corrLoading.set(true);
    this.corrTraces.set([]);
    this.api.searchTraces({
      from: new Date(e.tsMs - half).toISOString(),
      to:   new Date(e.tsMs + half).toISOString(),
      minDurationMs: Math.max(1, Math.floor(e.loMs)),
      maxDurationMs: isFinite(e.hiMs) ? Math.ceil(e.hiMs) : undefined,
      limit: 50,
    }).pipe(catchError(() => of([] as TraceRowDto[]))).subscribe(rows => {
      this.corrTraces.set([...rows].sort((a, b) => b.durationNanos - a.durationNanos));
      this.corrLoading.set(false);
      this.cdr.markForCheck();
    });
  }
  closeCorr() { this.corrOpen.set(false); }
  openTrace(traceId: string) { this.router.navigate(['/traces'], { queryParams: { trace: traceId } }); }

  // ── Time helpers ──────────────────────────────────────────────────────────
  private hoursOf(p: string): number { return PRESETS.find(x => x[0] === p)?.[1] ?? 1; }
  private fromIso(): string {
    if (this.preset() === 'custom') return localToIso(this.customFrom) || formatISO(subHours(new Date(), 1));
    return formatISO(subHours(new Date(), this.hoursOf(this.preset())));
  }
  private toIso(): string | undefined {
    return this.preset() === 'custom' ? (localToIso(this.customTo) || undefined) : undefined;
  }
  private parseFilters(raw: string): Record<string, string> | undefined {
    const out: Record<string, string> = {};
    for (const pair of (raw || '').split(',')) {
      const eq = pair.indexOf('=');
      if (eq <= 0) continue;
      const k = pair.slice(0, eq).trim(), v = pair.slice(eq + 1).trim();
      if (k && v) out[k] = v;
    }
    return Object.keys(out).length ? out : undefined;
  }

  // ── Chart ─────────────────────────────────────────────────────────────────
  private async draw(canvas: HTMLCanvasElement, series: MetricSeriesDto[]) {
    const seq = ++this.drawSeq;
    this.chart?.destroy();
    this.chart = null;

    const showEx = this.isHistogram() && this.showExemplars()
                   && this.viewMode() === 'lines' && this.mode() === 'query'
                   && this.exemplars.length > 0;
    if (!series.length && !showEx) return;

    const scale  = this.scale();
    const single = series.length === 1;

    const datasets: any[] = series.slice(0, 16).map((s, i) => {
      const color = this.seriesColor(s, i);
      return {
        label: this.seriesLabel(s),
        data: s.points.map(p => ({ x: Math.round(p.ts / 1e6), y: p.value * scale })),
        borderColor: color, backgroundColor: color + '22',
        fill: single && !showEx, tension: 0.25, pointRadius: 0, borderWidth: 1.5,
      };
    });

    const exIdx = datasets.length;
    if (showEx) {
      datasets.push({
        type: 'scatter', label: 'exemplars',
        data: this.exemplars.map(e => ({ x: Math.round(e.ts / 1e6), y: e.value * scale })),
        backgroundColor: '#f43f5e', borderColor: '#fff', borderWidth: 1,
        radius: 3.5, hoverRadius: 6, order: -1,
      });
    }

    const opts = lineOpts(!single || showEx);
    if (showEx) {
      opts.onClick = (_e: any, els: any[]) => {
        for (const el of els) if (el.datasetIndex === exIdx) {
          const t = this.exemplars[el.index]?.traceId; if (t) this.openTrace(t); return;
        }
      };
      opts.onHover = (e: any, els: any[]) => {
        const hit = els.some(el => el.datasetIndex === exIdx);
        const t = e?.native?.target; if (t) t.style.cursor = hit ? 'pointer' : 'default';
      };
    }

    const ChartCtor = await loadChart();
    if (seq !== this.drawSeq) return; // superseded by a newer draw while loading
    this.chart = new ChartCtor(canvas, { type: 'line', data: { datasets }, options: opts });
  }

  private seriesColor(s: MetricSeriesDto, i: number): string {
    const svc = s.labels['service.name'];
    return svc ? serviceColor(svc) : PALETTE[i % PALETTE.length];
  }

  seriesLabel(s: MetricSeriesDto): string {
    const entries = Object.entries(s.labels);
    if (!entries.length) return s.name;
    return entries.map(([k, v]) => `${k}=${v}`).join(', ').slice(0, 60);
  }

  // ── View helpers ──────────────────────────────────────────────────────────
  typeShort(t: string): string { return t === 'Histogram' ? 'H' : t === 'Counter' ? 'C' : 'G'; }
  typeColor(t: string): string { return t === 'Histogram' ? '#a78bfa' : t === 'Counter' ? '#38BDF8' : '#22C55E'; }
  fmtNum(n: number): string {
    if (!isFinite(n)) return '—';
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
    if (n >= 1_000)     return (n / 1_000).toFixed(1) + 'k';
    return String(n);
  }
  corrDur(nanos: number): string { return fmtMs(nanos / 1e6); }
  corrTime(nanos: number): string { return format(new Date(Math.round(nanos / 1e6)), 'HH:mm:ss.SSS'); }
}

// ── Module helpers ────────────────────────────────────────────────────────────
function defaultAgg(m: MetricCatalogDto): MetricAggregation {
  return m.type === 'Histogram' ? 'quantile' : m.type === 'Counter' ? 'rate' : 'avg';
}

function niceStepSec(rangeSec: number, target = 200): number {
  const ideal = rangeSec / target;
  for (const s of NICE_STEPS_SEC) if (s >= ideal) return s;
  return NICE_STEPS_SEC[NICE_STEPS_SEC.length - 1];
}
function fmtStep(sec: number): string {
  if (sec < 60) return sec + 's';
  if (sec < 3600) return (sec / 60) + 'm';
  return (sec / 3600) + 'h';
}
function fmtMs(ms: number): string {
  if (!isFinite(ms)) return '∞';
  return ms >= 1000 ? (ms / 1000).toFixed(2) + 's' : ms.toFixed(1) + 'ms';
}

/** datetime-local ("yyyy-MM-ddTHH:mm") → ISO-8601, or '' when empty/invalid. */
function localToIso(local: string): string {
  if (!local) return '';
  const d = new Date(local);
  return isNaN(d.getTime()) ? '' : d.toISOString();
}

function cssVar(name: string, fallback: string): string {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}

function lineOpts(showLegend: boolean): any {
  const grid    = cssVar('--border', '#1e293b');
  const tick    = cssVar('--text-muted', '#64748b');
  const legendC = cssVar('--text-secondary', '#94a3b8');
  const tipBg   = cssVar('--bg-elevated', '#0f172a');
  const tipText = cssVar('--text-primary', '#e2e8f0');
  return {
    responsive: true, maintainAspectRatio: false, animation: false,
    interaction: { mode: 'index', intersect: false },
    scales: {
      x: {
        type: 'linear',
        ticks: { color: tick, maxTicksLimit: 7, font: { size: 10 },
                 callback: (v: any) => format(new Date(Number(v)), 'HH:mm') },
        grid: { color: grid }, border: { display: false },
      },
      y: { ticks: { color: tick, font: { size: 10 } }, grid: { color: grid }, border: { display: false } },
    },
    plugins: {
      legend: { display: showLegend, position: 'bottom',
                labels: { color: legendC, font: { size: 10 }, boxWidth: 10, boxHeight: 10, usePointStyle: true } },
      tooltip: { backgroundColor: tipBg, borderColor: grid, borderWidth: 1,
                 titleColor: tipText, bodyColor: legendC, padding: 8,
                 callbacks: {
                   // The x scale is linear (ms epoch); without this the tooltip title
                   // would show the raw millisecond number instead of a readable time.
                   title: (items: any[]) => items.length
                     ? format(new Date(Number(items[0].parsed.x)), 'yyyy-MM-dd HH:mm:ss')
                     : '',
                 } },
      zoom: {
        pan:  { enabled: true, mode: 'x', modifierKey: 'shift' },
        zoom: { drag: { enabled: true, backgroundColor: 'rgba(56,189,248,0.15)', borderColor: '#38BDF8', borderWidth: 1 },
                wheel: { enabled: true }, mode: 'x' },
      },
    },
  };
}

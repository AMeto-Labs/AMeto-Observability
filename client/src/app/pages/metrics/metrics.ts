import {
  Component, signal, computed, inject, OnInit, OnDestroy,
  ChangeDetectionStrategy, ChangeDetectorRef, ViewChild, ElementRef,
} from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import { Chart, registerables } from 'chart.js';
import zoomPlugin from 'chartjs-plugin-zoom';
import { format, subHours, formatISO } from 'date-fns';
import { forkJoin, of } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { ApiService } from '../../core/services/api.service';
import {
  MetricSeriesDto, MetricCatalogDto, MetricQueryRequest, HeatmapDto, MetricAggregation, ExemplarDto,
} from '../../core/models/metric.model';
import { TraceRowDto } from '../../core/models/span.model';
import { HeatmapComponent } from './heatmap/heatmap';

Chart.register(...registerables, zoomPlugin);

type Tab = 'overview' | 'runtime' | 'explore';

interface StatCard {
  label: string; value: string; unit: string;
  spark: number[]; tone: 'ok' | 'warn' | 'error' | 'info';
  gaugePct?: number;
}
interface TopRow { label: string; value: number; pct: number; }
interface RuntimePanel { title: string; unit: string; series: MetricSeriesDto[]; canvasId: string; }

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

  private pendingExplore: { metric: string; agg: MetricAggregation; q: number; gb: string[] } | null = null;

  @ViewChild('rateCanvas') rateRef?: ElementRef<HTMLCanvasElement>;
  @ViewChild('latCanvas')  latRef?:  ElementRef<HTMLCanvasElement>;
  @ViewChild('exploreCanvas') exploreRef?: ElementRef<HTMLCanvasElement>;

  private charts: Chart[] = [];
  private runtimeCharts = new Map<string, Chart>();

  // ── State ─────────────────────────────────────────────────────────────────
  tab     = signal<Tab>('overview');
  preset  = signal('1h');
  loading = signal(false);

  // Overview
  reqMetric   = signal<string | null>(null);
  statCards   = signal<StatCard[]>([]);
  topRows     = signal<TopRow[]>([]);
  statusPcts  = signal<Record<string, number>>({ '2xx': 0, '3xx': 0, '4xx': 0, '5xx': 0 });
  heatmap     = signal<HeatmapDto | null>(null);
  latencyScale = signal(1);
  private rateSeries: MetricSeriesDto[] = [];
  private latSeries:  MetricSeriesDto[] = [];
  private exemplars:  ExemplarDto[]     = [];
  exemplarCount = signal(0);

  // Runtime
  runtimePanels = signal<RuntimePanel[]>([]);

  // Explore
  catalog       = signal<MetricCatalogDto[]>([]);
  catalogSearch = signal('');
  selectedMetric = signal<MetricCatalogDto | null>(null);
  exAggregation = signal<MetricAggregation>('rate');
  exQuantile    = signal(0.95);
  exGroupBy     = signal<string[]>([]);
  exHeatmap     = signal<HeatmapDto | null>(null);
  exLoading     = signal(false);
  comparePrev   = signal(false);

  // Expression mode (A op B)
  exprMode    = signal(false);
  exprLeft    = { metric: '', agg: 'rate' as MetricAggregation, filters: '' };
  exprRight   = { metric: '', agg: 'rate' as MetricAggregation, filters: '' };
  exprOp      = signal<'div' | 'mul' | 'add' | 'sub'>('div');
  exprScale   = signal(100);

  // Correlation drawer (metric → traces → logs)
  corrOpen    = signal(false);
  corrLoading = signal(false);
  corrTitle   = signal('');
  corrSub     = signal('');
  corrTraces  = signal<TraceRowDto[]>([]);

  filteredCatalog = computed(() => {
    const q = this.catalogSearch().toLowerCase();
    const c = this.catalog();
    return q ? c.filter(m => m.name.toLowerCase().includes(q)) : c;
  });

  donutDash = computed(() => {
    const p = this.statusPcts();
    const total = p['2xx'] + p['3xx'] + p['4xx'] + p['5xx'] || 1;
    const C = 2 * Math.PI * 30; // r=30
    let offset = 0;
    return (['2xx', '3xx', '4xx', '5xx'] as const).map(k => {
      const frac = p[k] / total;
      const seg = { key: k, dash: frac * C, offset: -offset * C, color: this.statusColor(k) };
      offset += frac;
      return seg;
    });
  });

  private _poll: ReturnType<typeof setInterval> | null = null;

  // ── Lifecycle ─────────────────────────────────────────────────────────────
  ngOnInit() {
    this.restoreFromUrl();
    this.loadCatalog();
    if (this.tab() === 'overview') this.loadOverview();
    else if (this.tab() === 'runtime') this.loadRuntime();
    this._poll = setInterval(() => this.refresh(), 30_000);
  }
  ngOnDestroy() {
    this.destroyCharts();
    this.runtimeCharts.forEach(c => c.destroy());
    if (this._poll) clearInterval(this._poll);
  }

  // ── URL state sync (survives F5 / deep-link) ──────────────────────────────
  private restoreFromUrl() {
    const q = this.route.snapshot.queryParamMap;
    const tab = q.get('tab');
    if (tab === 'runtime' || tab === 'explore' || tab === 'overview') this.tab.set(tab);
    const range = q.get('range');
    if (range) this.preset.set(range);

    const metric = q.get('metric');
    if (metric) {
      this.pendingExplore = {
        metric,
        agg: (q.get('agg') as MetricAggregation) || 'rate',
        q:   q.get('q') ? +q.get('q')! : 0.95,
        gb:  q.get('gb') ? q.get('gb')!.split(',').filter(Boolean) : [],
      };
      if (this.tab() !== 'explore') this.tab.set('explore');
    }
  }

  private syncUrl() {
    const inExplore = this.tab() === 'explore';
    const sel = this.selectedMetric();
    const qp: Record<string, string | null> = {
      tab:    this.tab() === 'overview' ? null : this.tab(),
      range:  this.preset() === '1h' ? null : this.preset(),
      metric: inExplore && sel ? sel.name : null,
      agg:    inExplore && sel ? this.exAggregation() : null,
      q:      inExplore && sel && this.exAggregation() === 'quantile' ? String(this.exQuantile()) : null,
      gb:     inExplore && sel && this.exGroupBy().length ? this.exGroupBy().join(',') : null,
    };
    this.router.navigate([], {
      relativeTo: this.route, queryParams: qp, queryParamsHandling: 'merge', replaceUrl: true,
    });
  }

  setTab(t: Tab) {
    this.tab.set(t);
    this.syncUrl();
    if (t === 'overview') this.loadOverview();
    else if (t === 'runtime') this.loadRuntime();
    else this.loadCatalog();
  }
  onPreset(p: string) { this.preset.set(p); this.syncUrl(); this.refresh(); }
  private refresh() {
    if (this.tab() === 'overview') this.loadOverview();
    else if (this.tab() === 'runtime') this.loadRuntime();
    else if (this.tab() === 'explore' && this.selectedMetric()) this.runExplore();
  }

  // ── Time helpers ──────────────────────────────────────────────────────────
  private hours(): number {
    const m: Record<string, number> = { '15m': 0.25, '30m': 0.5, '1h': 1, '3h': 3, '6h': 6, '12h': 12, '24h': 24 };
    return m[this.preset()] ?? 1;
  }
  private fromIso(): string { return formatISO(subHours(new Date(), this.hours())); }
  private step(): string {
    const h = this.hours();
    if (h <= 0.5) return '15s';
    if (h <= 1)   return '30s';
    if (h <= 6)   return '2m';
    if (h <= 12)  return '5m';
    return '10m';
  }
  private stepMs(): number {
    const s = this.step();
    const n = parseFloat(s);
    return s.endsWith('m') ? n * 60_000 : n * 1000;
  }

  // ── Correlation: metric point → traces in window → trace detail ────────────
  /** Heatmap cell clicked: find traces in this time bucket with matching latency. */
  onHeatmapCell(e: { tsMs: number; loMs: number; hiMs: number; count: number }) {
    const half = this.stepMs();
    const from = new Date(e.tsMs - half).toISOString();
    const to   = new Date(e.tsMs + half).toISOString();
    const hiLabel = isFinite(e.hiMs) ? `${fmt(e.hiMs)}ms` : '∞';
    this.corrTitle.set(`Traces · ${fmt(e.loMs)}–${hiLabel}`);
    this.corrSub.set(`around ${format(new Date(e.tsMs), 'HH:mm:ss')} · ${e.count} samples`);
    this.openCorr({
      from, to,
      minDurationMs: Math.max(1, Math.floor(e.loMs)),
      maxDurationMs: isFinite(e.hiMs) ? Math.ceil(e.hiMs) : undefined,
    });
  }

  private openCorr(params: { from: string; to: string; minDurationMs?: number; maxDurationMs?: number }) {
    this.corrOpen.set(true);
    this.corrLoading.set(true);
    this.corrTraces.set([]);
    this.api.searchTraces({ ...params, limit: 50 }).pipe(catchError(() => of([] as TraceRowDto[])))
      .subscribe(rows => {
        this.corrTraces.set(rows.sort((a, b) => b.durationNanos - a.durationNanos));
        this.corrLoading.set(false);
        this.cdr.markForCheck();
      });
  }

  closeCorr() { this.corrOpen.set(false); }

  /** Jump to the trace detail (Traces page opens it via ?trace= URL state). */
  openTrace(traceId: string) {
    this.router.navigate(['/traces'], { queryParams: { trace: traceId } });
  }

  corrFmtTime(nanos: number): string { return format(new Date(Math.round(nanos / 1e6)), 'HH:mm:ss.SSS'); }
  corrFmtDur(nanos: number): string {
    const ms = nanos / 1e6;
    return ms >= 1000 ? (ms / 1000).toFixed(2) + 's' : ms.toFixed(1) + 'ms';
  }

  // ── Catalog ───────────────────────────────────────────────────────────────
  private loadCatalog() {
    this.api.getMetricCatalog().pipe(catchError(() => of([] as MetricCatalogDto[]))).subscribe(c => {
      this.catalog.set(c);
      if (!this.reqMetric()) {
        const req = c.find(m => m.type === 'Histogram' && /request.*duration|http.*server.*duration/i.test(m.name));
        if (req) { this.reqMetric.set(req.name); this.latencyScale.set(req.unit === 's' ? 1000 : 1); }
      }
      this.cdr.markForCheck();

      // Restore a deep-linked Explore selection once the catalog is available.
      if (this.pendingExplore) {
        const pe = this.pendingExplore;
        this.pendingExplore = null;
        const m = c.find(x => x.name === pe.metric);
        if (m) {
          this.selectedMetric.set(m);
          this.exAggregation.set(pe.agg);
          this.exQuantile.set(pe.q);
          this.exGroupBy.set(pe.gb);
          this.runExplore();
        }
      }

      if (this.tab() === 'overview') this.loadOverview();
    });
  }

  // ── Overview (RED) ────────────────────────────────────────────────────────
  loadOverview() {
    const metric = this.reqMetric();
    if (!metric) return;
    this.loading.set(true);
    const from = this.fromIso(), step = this.step();
    const scale = this.latencyScale();

    const q = (req: Partial<MetricQueryRequest>) =>
      this.api.queryMetricAgg({ metric, from, step, ...req } as MetricQueryRequest)
        .pipe(catchError(() => of([] as MetricSeriesDto[])));

    forkJoin({
      rate:   q({ aggregation: 'rate' }),
      p50:    q({ aggregation: 'quantile', quantile: 0.50 }),
      p95:    q({ aggregation: 'quantile', quantile: 0.95 }),
      p99:    q({ aggregation: 'quantile', quantile: 0.99 }),
      byCode: q({ aggregation: 'rate', groupBy: ['http.response.status_code'] }),
      byRoute: q({ aggregation: 'rate', groupBy: ['http.route'], topk: 8 }),
      heat:   this.api.getMetricHeatmap(metric, from, undefined, step).pipe(catchError(() => of(null as any))),
      exemplars: this.api.getMetricExemplars(metric, from, undefined, undefined, 500).pipe(catchError(() => of([] as ExemplarDto[]))),
    }).subscribe(r => {
      this.exemplars = r.exemplars;
      this.exemplarCount.set(r.exemplars.length);
      this.rateSeries = r.rate;
      this.latSeries  = [
        this.renameSeries(r.p50, 'p50'),
        this.renameSeries(r.p95, 'p95'),
        this.renameSeries(r.p99, 'p99'),
      ].flat();
      this.heatmap.set(r.heat);

      // Stat cards
      const reqRate = this.lastSum(r.rate);
      const errRate = this.sumByClass(r.byCode, '5');
      const errPct  = reqRate > 0 ? (errRate / reqRate) * 100 : 0;
      this.statCards.set([
        { label: 'Request rate', value: fmt(reqRate), unit: 'rps', tone: 'info',  spark: this.sparkSum(r.rate) },
        { label: 'Error rate',   value: errPct.toFixed(2), unit: '%', tone: errPct > 5 ? 'error' : errPct > 1 ? 'warn' : 'ok', spark: [], gaugePct: Math.min(100, errPct) },
        { label: 'p50',  value: fmt(this.lastVal(r.p50) * scale), unit: 'ms', tone: 'ok',   spark: this.spark(r.p50, scale) },
        { label: 'p95',  value: fmt(this.lastVal(r.p95) * scale), unit: 'ms', tone: 'warn', spark: this.spark(r.p95, scale) },
        { label: 'p99',  value: fmt(this.lastVal(r.p99) * scale), unit: 'ms', tone: 'error', spark: this.spark(r.p99, scale) },
      ]);

      // Status donut
      const cls: Record<string, number> = { '2xx': 0, '3xx': 0, '4xx': 0, '5xx': 0 };
      for (const s of r.byCode) {
        const code = s.labels['http.response.status_code'] || '';
        const v = this.lastVal([s]);
        if (code.startsWith('2')) cls['2xx'] += v;
        else if (code.startsWith('3')) cls['3xx'] += v;
        else if (code.startsWith('4')) cls['4xx'] += v;
        else if (code.startsWith('5')) cls['5xx'] += v;
      }
      this.statusPcts.set(cls);

      // Top-K routes
      const rows = r.byRoute.map(s => ({
        label: s.labels['http.route'] || s.name, value: this.lastVal([s]), pct: 0,
      })).sort((a, b) => b.value - a.value);
      const max = rows[0]?.value || 1;
      rows.forEach(rr => rr.pct = (rr.value / max) * 100);
      this.topRows.set(rows);

      this.loading.set(false);
      this.cdr.detectChanges();
      setTimeout(() => { this.renderOverviewCharts(); this.cdr.markForCheck(); }, 0);
    });
  }

  private renderOverviewCharts() {
    this.destroyCharts();
    if (this.rateRef) this.charts.push(this.lineChart(this.rateRef.nativeElement, this.rateSeries, ['#38BDF8'], 1));
    if (this.latRef)  this.charts.push(this.latencyChart(this.latRef.nativeElement, this.latSeries, this.latencyScale(), this.exemplars));
  }

  /** Latency percentile lines + exemplar dots (click a dot → jump to its trace). */
  private latencyChart(canvas: HTMLCanvasElement, series: MetricSeriesDto[], scale: number, exemplars: ExemplarDto[]): Chart {
    const colors = ['#22C55E', '#F59E0B', '#EF4444'];
    const datasets: any[] = series.slice(0, 12).map((s, i) => {
      const color = colors[i % colors.length];
      return {
        type: 'line', label: this.seriesLabel(s),
        data: s.points.map(p => ({ x: Math.round(p.ts / 1e6), y: p.value * scale })),
        borderColor: color, backgroundColor: color + '18',
        fill: false, tension: 0.3, pointRadius: 0, borderWidth: 1.5,
      };
    });
    const exIdx = datasets.length;
    if (exemplars.length) {
      datasets.push({
        type: 'scatter', label: 'exemplars',
        data: exemplars.map(e => ({ x: Math.round(e.ts / 1e6), y: e.value * scale })),
        backgroundColor: '#f43f5e', borderColor: '#fff', borderWidth: 1,
        radius: 4, hoverRadius: 6, order: -1,
      });
    }
    const opts = lineOpts(true);
    opts.onClick = (_e: any, els: any[]) => {
      for (const el of els) {
        if (el.datasetIndex === exIdx) {
          const tid = exemplars[el.index]?.traceId;
          if (tid) this.openTrace(tid);
          return;
        }
      }
    };
    opts.onHover = (e: any, els: any[]) => {
      const hit = els.some(el => el.datasetIndex === exIdx);
      const t = e?.native?.target; if (t) t.style.cursor = hit ? 'pointer' : 'default';
    };
    return new Chart(canvas, { type: 'line', data: { datasets }, options: opts });
  }

  // ── Runtime (USE) ─────────────────────────────────────────────────────────
  loadRuntime() {
    const cat = this.catalog();
    const from = this.fromIso(), step = this.step();
    const want: { title: string; rx: RegExp; agg: MetricAggregation }[] = [
      { title: 'CPU', rx: /process.*cpu|cpu.*utilization|cpu.*time/i, agg: 'avg' },
      { title: 'Memory (working set)', rx: /process.*memory|working_set|memory.*usage/i, agg: 'avg' },
      { title: 'GC heap', rx: /gc.*heap|dotnet.*gc.*heap|heap.*size/i, agg: 'avg' },
      { title: 'GC collections', rx: /gc.*collection|gc.*count/i, agg: 'rate' },
      { title: 'ThreadPool threads', rx: /thread_pool.*thread|threadpool.*count|thread.*count/i, agg: 'avg' },
      { title: 'Active connections', rx: /kestrel.*connection|active_connection/i, agg: 'avg' },
      { title: 'Active requests', rx: /active_request|http.*server.*active/i, agg: 'avg' },
    ];
    const tasks = want.map(w => {
      const m = cat.find(c => w.rx.test(c.name));
      if (!m) return of(null);
      const agg: MetricAggregation = m.type === 'Counter' ? 'rate' : w.agg;
      return this.api.queryMetricAgg({ metric: m.name, from, step, aggregation: agg })
        .pipe(catchError(() => of([] as MetricSeriesDto[])),
              // attach meta
              );
    });
    if (!tasks.length) return;
    this.loading.set(true);
    forkJoin(tasks).subscribe(results => {
      const panels: RuntimePanel[] = [];
      results.forEach((series, i) => {
        if (!series || !series.length) return;
        panels.push({ title: want[i].title, unit: series[0]?.unit ?? '', series, canvasId: 'rt_' + i });
      });
      this.runtimePanels.set(panels);
      this.loading.set(false);
      this.cdr.detectChanges();
      setTimeout(() => { this.renderRuntimeCharts(); this.cdr.markForCheck(); }, 0);
    });
  }

  private renderRuntimeCharts() {
    this.runtimeCharts.forEach(c => c.destroy());
    this.runtimeCharts.clear();
    for (const p of this.runtimePanels()) {
      const canvas = document.getElementById(p.canvasId) as HTMLCanvasElement | null;
      if (!canvas) continue;
      this.runtimeCharts.set(p.canvasId, this.lineChart(canvas, p.series, ['#a78bfa', '#38BDF8', '#22C55E'], 1));
    }
  }

  // ── Explore ───────────────────────────────────────────────────────────────
  selectMetric(m: MetricCatalogDto) {
    this.selectedMetric.set(m);
    this.exGroupBy.set([]);
    this.exAggregation.set(m.type === 'Histogram' ? 'quantile' : m.type === 'Counter' ? 'rate' : 'avg');
    this.runExplore();
  }
  toggleGroupBy(key: string) {
    const cur = this.exGroupBy();
    this.exGroupBy.set(cur.includes(key) ? cur.filter(k => k !== key) : [...cur, key]);
  }
  runExplore() {
    if (this.exprMode()) { this.runExpr(); return; }
    const m = this.selectedMetric();
    if (!m) return;
    this.syncUrl();
    this.exLoading.set(true);
    this.exHeatmap.set(null);
    const from = this.fromIso(), step = this.step();
    const agg = this.exAggregation();
    const scale = m.type === 'Histogram' && m.unit === 's' ? 1000 : 1;

    if (m.type === 'Histogram' && agg === 'quantile' && this.exGroupBy().length === 0) {
      this.api.getMetricHeatmap(m.name, from, undefined, step)
        .pipe(catchError(() => of(null as any))).subscribe(h => { this.exHeatmap.set(h); this.cdr.markForCheck(); });
    }

    const req: MetricQueryRequest = {
      metric: m.name, from, step, aggregation: agg,
      quantile: agg === 'quantile' ? this.exQuantile() : undefined,
      groupBy: this.exGroupBy().length ? this.exGroupBy() : undefined,
      topk: 12,
    };
    const main$ = this.api.queryMetricAgg(req).pipe(catchError(() => of([] as MetricSeriesDto[])));

    // Compare-to-previous-period: shift a prior window forward onto the same axis.
    const hrs = this.hours();
    const prev$ = this.comparePrev()
      ? this.api.queryMetricAgg({ ...req, from: formatISO(subHours(new Date(), hrs * 2)), to: from })
          .pipe(catchError(() => of([] as MetricSeriesDto[])))
      : of([] as MetricSeriesDto[]);

    forkJoin({ main: main$, prev: prev$ }).subscribe(({ main, prev }) => {
      this.exLoading.set(false);
      this.cdr.detectChanges();
      setTimeout(() => {
        const canvas = this.exploreRef?.nativeElement;
        if (canvas) {
          this.destroyChart(canvas);
          this.charts.push(this.lineChart(canvas, main, PALETTE, scale, this.shiftSeries(prev, hrs)));
        }
        this.cdr.markForCheck();
      }, 0);
    });
  }

  /** Expression mode: evaluate left op right and graph the single result series. */
  runExpr() {
    if (!this.exprLeft.metric || !this.exprRight.metric) return;
    this.exLoading.set(true);
    this.exHeatmap.set(null);
    const from = this.fromIso(), step = this.step();
    const side = (s: { metric: string; agg: MetricAggregation; filters: string }): MetricQueryRequest =>
      ({ metric: s.metric, from, step, aggregation: s.agg, filters: this.parseFilters(s.filters) });
    this.api.queryMetricExpr({
      left: side(this.exprLeft), right: side(this.exprRight),
      op: this.exprOp(), scale: this.exprScale(), name: 'expression',
    }).pipe(catchError(() => of(null as any))).subscribe((series: MetricSeriesDto | null) => {
      this.exLoading.set(false);
      this.cdr.detectChanges();
      setTimeout(() => {
        const canvas = this.exploreRef?.nativeElement;
        if (canvas) {
          this.destroyChart(canvas);
          this.charts.push(this.lineChart(canvas, series ? [series] : [], ['#38BDF8'], 1));
        }
        this.cdr.markForCheck();
      }, 0);
    });
  }

  toggleExprMode() { this.exprMode.update(v => !v); }
  resetZoom() { for (const c of this.charts) (c as any).resetZoom?.(); }

  /** Parse "key=value, key2=value2" into a label-matcher map (undefined if empty). */
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

  private shiftSeries(series: MetricSeriesDto[], hours: number): MetricSeriesDto[] {
    const shiftMs = hours * 3600_000;
    return series.map(s => ({ ...s, name: 'prev · ' + s.name,
      points: s.points.map(p => ({ ...p, ts: p.ts + shiftMs * 1e6 })) }));
  }

  // ── Chart.js helpers ──────────────────────────────────────────────────────
  private lineChart(canvas: HTMLCanvasElement, series: MetricSeriesDto[], colors: string[], scale: number,
                    compare: MetricSeriesDto[] = []): Chart {
    const datasets: any[] = series.slice(0, 12).map((s, i) => {
      const color = colors[i % colors.length];
      return {
        label: this.seriesLabel(s),
        data: s.points.map(p => ({ x: Math.round(p.ts / 1e6), y: p.value * scale })),
        borderColor: color, backgroundColor: color + '18',
        fill: series.length === 1 && !compare.length, tension: 0.3, pointRadius: 0, borderWidth: 1.5,
      };
    });
    for (const s of compare.slice(0, 12)) {
      datasets.push({
        label: this.seriesLabel(s),
        data: s.points.map(p => ({ x: Math.round(p.ts / 1e6), y: p.value * scale })),
        borderColor: '#64748b', borderDash: [4, 4], fill: false, tension: 0.3, pointRadius: 0, borderWidth: 1,
      });
    }
    const showLegend = (series.length + compare.length) > 1;
    return new Chart(canvas, { type: 'line', data: { datasets }, options: lineOpts(showLegend) });
  }
  private destroyChart(canvas: HTMLCanvasElement) {
    const existing = Chart.getChart(canvas);
    if (existing) existing.destroy();
  }
  private destroyCharts() { this.charts.forEach(c => c.destroy()); this.charts = []; }

  // ── Aggregation helpers ───────────────────────────────────────────────────
  private lastVal(series: MetricSeriesDto[]): number {
    let v = 0;
    for (const s of series) { const p = s.points[s.points.length - 1]; if (p) v += p.value; }
    return v;
  }
  private lastSum(series: MetricSeriesDto[]): number { return this.lastVal(series); }
  private sumByClass(series: MetricSeriesDto[], prefix: string): number {
    let v = 0;
    for (const s of series) {
      const code = s.labels['http.response.status_code'] || '';
      if (code.startsWith(prefix)) { const p = s.points[s.points.length - 1]; if (p) v += p.value; }
    }
    return v;
  }
  private spark(series: MetricSeriesDto[], scale: number): number[] {
    return (series[0]?.points ?? []).slice(-20).map(p => p.value * scale);
  }
  private sparkSum(series: MetricSeriesDto[]): number[] {
    const merged: Record<number, number> = {};
    for (const s of series) for (const p of s.points) merged[p.ts] = (merged[p.ts] ?? 0) + p.value;
    return Object.keys(merged).map(Number).sort((a, b) => a - b).slice(-20).map(k => merged[k]);
  }
  private renameSeries(series: MetricSeriesDto[], name: string): MetricSeriesDto[] {
    return series.map(s => ({ ...s, name }));
  }
  seriesLabel(s: MetricSeriesDto): string {
    const entries = Object.entries(s.labels);
    if (!entries.length) return s.name;
    return entries.map(([k, v]) => `${k}=${v}`).join(', ').slice(0, 48) || s.name;
  }

  // ── Formatting ────────────────────────────────────────────────────────────
  fmtCard(v: string): string { return v; }
  fmtCardinality(n: number): string { return fmt(n); }
  fmtLastSeen(ms: number): string {
    if (!ms) return '—';
    const d = Date.now() - ms;
    if (d < 60_000) return Math.round(d / 1000) + 's';
    if (d < 3_600_000) return Math.round(d / 60_000) + 'm';
    return Math.round(d / 3_600_000) + 'h';
  }
  statusColor(k: string): string {
    return k === '2xx' ? '#22C55E' : k === '3xx' ? '#38BDF8' : k === '4xx' ? '#F59E0B' : '#EF4444';
  }
  toneColor(t: string): string {
    return t === 'error' ? '#EF4444' : t === 'warn' ? '#F59E0B' : t === 'ok' ? '#22C55E' : '#38BDF8';
  }
  sparkPath(data: number[]): string {
    if (data.length < 2) return '';
    const max = Math.max(...data, 1), min = Math.min(...data, 0);
    const range = max - min || 1;
    const W = 100, H = 28;
    return data.map((v, i) => `${(i / (data.length - 1)) * W},${H - ((v - min) / range) * H}`).join(' ');
  }
  catalogTone(type: string): string {
    return type === 'Histogram' ? '#a78bfa' : type === 'Counter' ? '#38BDF8' : '#22C55E';
  }
}

const PALETTE = ['#38BDF8', '#F59E0B', '#22C55E', '#a78bfa', '#f97316', '#ec4899', '#06b6d4', '#84cc16'];

function fmt(v: number): string {
  if (!isFinite(v)) return '—';
  if (Math.abs(v) >= 1_000_000) return (v / 1_000_000).toFixed(2) + 'M';
  if (Math.abs(v) >= 1_000)     return (v / 1_000).toFixed(2) + 'k';
  if (Number.isInteger(v))      return String(v);
  return v.toFixed(2);
}

function lineOpts(showLegend: boolean): any {
  return {
    responsive: true, maintainAspectRatio: false, animation: false,
    interaction: { mode: 'index', intersect: false },
    scales: {
      x: { type: 'linear', ticks: { color: '#64748b', maxTicksLimit: 6, font: { size: 10 }, callback: (v: any) => format(new Date(Number(v)), 'HH:mm') }, grid: { color: '#1e293b' }, border: { display: false } },
      y: { ticks: { color: '#64748b', font: { size: 10 } }, grid: { color: '#1e293b' }, border: { display: false } },
    },
    plugins: {
      legend: { display: showLegend, labels: { color: '#94a3b8', font: { size: 10 }, boxWidth: 10, boxHeight: 10 } },
      tooltip: { backgroundColor: '#0f172a', borderColor: '#263244', borderWidth: 1, titleColor: '#e2e8f0', bodyColor: '#94a3b8', padding: 8 },
      zoom: {
        pan:  { enabled: true, mode: 'x', modifierKey: 'shift' },
        zoom: { drag: { enabled: true, backgroundColor: 'rgba(56,189,248,0.15)', borderColor: '#38BDF8', borderWidth: 1 },
                wheel: { enabled: true }, mode: 'x' },
      },
    },
  };
}

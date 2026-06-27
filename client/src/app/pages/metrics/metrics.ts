import {
  Component, signal, computed, inject, OnInit, OnDestroy,
  ChangeDetectionStrategy, ChangeDetectorRef, ViewChild, ElementRef,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import { Chart, registerables } from 'chart.js';
import { format, subHours, formatISO } from 'date-fns';
import { forkJoin, of } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { ApiService } from '../../core/services/api.service';
import {
  MetricSeriesDto, MetricCatalogDto, MetricQueryRequest, HeatmapDto, MetricAggregation,
} from '../../core/models/metric.model';
import { HeatmapComponent } from './heatmap/heatmap';

Chart.register(...registerables);

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
  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);

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
    this.loadCatalog();
    this.loadOverview();
    this._poll = setInterval(() => this.refresh(), 30_000);
  }
  ngOnDestroy() {
    this.destroyCharts();
    this.runtimeCharts.forEach(c => c.destroy());
    if (this._poll) clearInterval(this._poll);
  }

  setTab(t: Tab) {
    this.tab.set(t);
    if (t === 'overview') this.loadOverview();
    else if (t === 'runtime') this.loadRuntime();
    else this.loadCatalog();
  }
  onPreset(p: string) { this.preset.set(p); this.refresh(); }
  private refresh() {
    if (this.tab() === 'overview') this.loadOverview();
    else if (this.tab() === 'runtime') this.loadRuntime();
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

  // ── Catalog ───────────────────────────────────────────────────────────────
  private loadCatalog() {
    this.api.getMetricCatalog().pipe(catchError(() => of([] as MetricCatalogDto[]))).subscribe(c => {
      this.catalog.set(c);
      if (!this.reqMetric()) {
        const req = c.find(m => m.type === 'Histogram' && /request.*duration|http.*server.*duration/i.test(m.name));
        if (req) { this.reqMetric.set(req.name); this.latencyScale.set(req.unit === 's' ? 1000 : 1); }
      }
      this.cdr.markForCheck();
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
    }).subscribe(r => {
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
    if (this.latRef)  this.charts.push(this.lineChart(this.latRef.nativeElement, this.latSeries, ['#22C55E', '#F59E0B', '#EF4444'], this.latencyScale()));
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
    const m = this.selectedMetric();
    if (!m) return;
    this.exLoading.set(true);
    this.exHeatmap.set(null);
    const from = this.fromIso(), step = this.step();
    const agg = this.exAggregation();

    if (m.type === 'Histogram' && agg === 'quantile' && this.exGroupBy().length === 0) {
      // also offer heatmap alongside the percentile line
      this.api.getMetricHeatmap(m.name, from, undefined, step)
        .pipe(catchError(() => of(null as any))).subscribe(h => { this.exHeatmap.set(h); this.cdr.markForCheck(); });
    }

    this.api.queryMetricAgg({
      metric: m.name, from, step, aggregation: agg,
      quantile: agg === 'quantile' ? this.exQuantile() : undefined,
      groupBy: this.exGroupBy().length ? this.exGroupBy() : undefined,
      topk: 12,
    }).pipe(catchError(() => of([] as MetricSeriesDto[]))).subscribe(series => {
      this.exLoading.set(false);
      this.cdr.detectChanges();
      setTimeout(() => {
        const canvas = this.exploreRef?.nativeElement;
        if (canvas) {
          this.destroyChart(canvas);
          const scale = m.type === 'Histogram' && m.unit === 's' ? 1000 : 1;
          this.charts.push(this.lineChart(canvas, series, PALETTE, scale));
        }
        this.cdr.markForCheck();
      }, 0);
    });
  }

  // ── Chart.js helpers ──────────────────────────────────────────────────────
  private lineChart(canvas: HTMLCanvasElement, series: MetricSeriesDto[], colors: string[], scale: number): Chart {
    const datasets = series.slice(0, 12).map((s, i) => {
      const color = colors[i % colors.length];
      return {
        label: this.seriesLabel(s),
        data: s.points.map(p => ({ x: Math.round(p.ts / 1e6), y: p.value * scale })),
        borderColor: color, backgroundColor: color + '18',
        fill: series.length === 1, tension: 0.3, pointRadius: 0, borderWidth: 1.5,
      };
    });
    return new Chart(canvas, { type: 'line', data: { datasets }, options: lineOpts(series.length > 1) });
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
    },
  };
}

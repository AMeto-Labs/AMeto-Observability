import {
  Component, signal, computed, inject, OnInit, OnDestroy,
  ChangeDetectionStrategy, ChangeDetectorRef, ViewChild, ElementRef,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import { Chart, registerables } from 'chart.js';
import { format } from 'date-fns';
import { ApiService } from '../../core/services/api.service';
import { MetricSeriesDto } from '../../core/models/metric.model';
import { subHours, formatISO } from 'date-fns';
import { forkJoin, of } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { SparklineComponent } from '../../shared/sparkline/sparkline.component';

Chart.register(...registerables);

interface StatCard {
  label: string;
  value: string;
  unit: string;
  change: number;
  changePositive: boolean;
  trendUp: boolean;
  sparkline: number[];
}

interface RecentMetricRow {
  name: string;
  kind: string;
  unit: string;
  service: string;
  lastValue: string;
  lastTs: number;
  change: number;
  trendUp: boolean;
  sparkline: number[];
  cardinality: number;
}

interface MetricGroup {
  ns: string;
  count: number;
  collapsed: boolean;
  items: RecentMetricRow[];
}

interface ServiceRow {
  name: string;
  value: number;
  pct: number;
}

@Component({
  selector: 'app-metrics',
  imports: [FormsModule, LucideAngularModule, SparklineComponent],
  templateUrl: './metrics.html',
  styleUrl: './metrics.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class MetricsComponent implements OnInit, OnDestroy {
  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);

  // Chart canvases
  @ViewChild('reqCanvas')   reqRef?:   ElementRef<HTMLCanvasElement>;
  @ViewChild('latCanvas')   latRef?:   ElementRef<HTMLCanvasElement>;
  @ViewChild('errCanvas')   errRef?:   ElementRef<HTMLCanvasElement>;
  @ViewChild('cpuCanvas')   cpuRef?:   ElementRef<HTMLCanvasElement>;
  @ViewChild('memCanvas')   memRef?:   ElementRef<HTMLCanvasElement>;
  @ViewChild('donutCanvas') donutRef?: ElementRef<HTMLCanvasElement>;

  private charts: Chart[] = [];
  private inlineCharts = new Map<string, Chart>();
  private _series: Record<string, MetricSeriesDto[]> = {};

  // ── State ─────────────────────────────────────────────────────────────────
  loading     = signal(false);
  statCards   = signal<StatCard[]>(this.emptyCards());
  allNames    = signal<string[]>([]);
  recentRows  = signal<RecentMetricRow[]>([]);
  topServices = signal<ServiceRow[]>([]);
  statusPcts  = signal<Record<string, string>>({ '2xx': '0', '3xx': '0', '4xx': '0', '5xx': '0' });

  // Filters
  preset        = signal('1h');
  viewMode      = signal<'timeseries' | 'table' | 'heatmap'>('timeseries');
  metricSearch  = signal('');
  serviceFilter = signal('');
  allServices   = signal<string[]>([]);

  // Sorting & expand
  sortCol         = signal<string>('name');
  sortAsc         = signal<boolean>(true);
  expandedRow     = signal<string | null>(null);
  collapsedGroups = signal<Set<string>>(new Set());

  filteredRecent = computed(() => {
    const q   = this.metricSearch().toLowerCase();
    const svc = this.serviceFilter();
    return this.recentRows().filter(r =>
      (!q || r.name.toLowerCase().includes(q)) &&
      (!svc || r.service === svc)
    );
  });

  sortedFiltered = computed(() => {
    const rows = this.filteredRecent();
    const col  = this.sortCol();
    const asc  = this.sortAsc();
    return [...rows].sort((a, b) => {
      let cmp = 0;
      if      (col === 'value')  cmp = (parseFloat(a.lastValue) || 0) - (parseFloat(b.lastValue) || 0);
      else if (col === 'change') cmp = a.change - b.change;
      else                       cmp = a.name.localeCompare(b.name);
      return asc ? cmp : -cmp;
    });
  });

  groupedMetrics = computed((): MetricGroup[] => {
    const rows      = this.sortedFiltered();
    const collapsed = this.collapsedGroups();
    const map = new Map<string, RecentMetricRow[]>();
    for (const row of rows) {
      const ns = row.name.includes('.') ? row.name.split('.')[0] : row.name;
      if (!map.has(ns)) map.set(ns, []);
      map.get(ns)!.push(row);
    }
    return Array.from(map.entries()).map(([ns, items]) => ({
      ns, count: items.length, collapsed: collapsed.has(ns), items,
    }));
  });

  get fromDateStr(): string {
    return format(subHours(new Date(), this.presetHours()), 'dd/MM/yyyy HH:mm:ss');
  }

  private _poll: ReturnType<typeof setInterval> | null = null;

  // ── Lifecycle ─────────────────────────────────────────────────────────────
  ngOnInit() {
    this.loadAll();
    this._poll = setInterval(() => this.loadAll(), 30_000);
  }

  ngOnDestroy() {
    this.destroyCharts();
    this.inlineCharts.forEach(c => c.destroy());
    this.inlineCharts.clear();
    if (this._poll) clearInterval(this._poll);
  }

  // ── Loading ───────────────────────────────────────────────────────────────
  loadAll() {
    this.loading.set(true);
    this.api.getMetricNames().subscribe({
      next: names => {
        this.allNames.set(names.sort());
        this.queryKeyMetrics(names);
      },
      error: () => { this.loading.set(false); this.cdr.markForCheck(); },
    });
  }

  private queryKeyMetrics(names: string[]) {
    const toLoad = this.pickMetrics(names);
    if (!toLoad.length) { this.finalize(names); return; }

    const from = formatISO(subHours(new Date(), this.presetHours()));
    forkJoin(toLoad.map(n => this.api.queryMetric(n, from).pipe(catchError(() => of([] as MetricSeriesDto[])))))
      .subscribe(results => {
        toLoad.forEach((n, i) => { this._series[n] = results[i]; });
        this.finalize(names);
      });
  }

  private pickMetrics(names: string[]): string[] {
    const picked: string[] = [];
    const patterns = [
      /http\.server\.request\.duration/i,
      /duration|latency/i,
      /error|exception/i,
      /http\.server\.active_requests|cpu|processor/i,
      /kestrel\.active_connections|memory|heap/i,
    ];
    for (const p of patterns) {
      const m = names.find(n => p.test(n));
      if (m && !picked.includes(m)) picked.push(m);
    }
    for (const n of names) {
      if (picked.length >= 12) break;
      if (!picked.includes(n)) picked.push(n);
    }
    return picked;
  }

  private finalize(names: string[]) {
    this.buildStatCards(names);
    this.buildRecentRows(names);
    this.buildTopServices();
    this.buildStatusPcts();
    this.loading.set(false);
    this.cdr.detectChanges();
    setTimeout(() => { this.renderCharts(); this.cdr.markForCheck(); }, 0);
  }

  // ── Stat cards ────────────────────────────────────────────────────────────
  private emptyCards(): StatCard[] {
    return [
      { label: 'Request rate',  value: '—', unit: 'rps', change: 0, changePositive: true,  trendUp: true,  sparkline: [] },
      { label: 'P50 latency',   value: '—', unit: 'ms',  change: 0, changePositive: true,  trendUp: false, sparkline: [] },
      { label: 'P95 latency',   value: '—', unit: 'ms',  change: 0, changePositive: false, trendUp: true,  sparkline: [] },
      { label: 'Error rate',    value: '—', unit: '%',   change: 0, changePositive: false, trendUp: true,  sparkline: [] },
      { label: 'Active series', value: '—', unit: '',    change: 0, changePositive: true,  trendUp: true,  sparkline: [] },
      { label: 'Active requests', value: '—', unit: '',    change: 0, changePositive: true,  trendUp: true,  sparkline: [] },
    ];
  }

  private buildStatCards(names: string[]) {
    const cards = this.emptyCards();
    cards[4].value = fmtK(names.length);

    const reqName = names.find(n => /http\.server\.request|request\.duration|http\.server/i.test(n));
    if (reqName && this._series[reqName]?.length) {
      const r = aggregateRate(this._series[reqName]);
      if (r.rate > 0) {
        cards[0] = { ...cards[0], value: fmtK(r.rate), sparkline: r.sparkline, change: +r.change.toFixed(1), trendUp: r.change > 0, changePositive: r.change > 0 };
      }

      const lat = computeLatency(this._series[reqName]);
      if (lat.p50 > 0) {
        cards[1] = { ...cards[1], value: lat.p50.toFixed(0), sparkline: lat.sparkline, change: +lat.change.toFixed(1), trendUp: lat.change > 0, changePositive: lat.change <= 0 };
        cards[2] = { ...cards[2], value: lat.p95.toFixed(0), change: +lat.change.toFixed(1), trendUp: lat.change > 0, changePositive: lat.change <= 0 };
      }
    }

    const errName = names.find(n => /error|exception/i.test(n));
    if (errName && this._series[errName]?.length) {
      const r = aggregateRate(this._series[errName]);
      const reqRate = parseFloat(cards[0].value.replace('k', '000').replace('M', '000000')) || 1;
      const pct = reqRate > 0 ? Math.min(100, (r.rate / reqRate) * 100) : 0;
      cards[3] = { ...cards[3], value: pct.toFixed(2), change: +r.change.toFixed(1), trendUp: r.change > 0, changePositive: r.change <= 0 };
    }

    const activeReqName = names.find(n => /http\.server\.active_requests|cpu/i.test(n));
    if (activeReqName && this._series[activeReqName]?.length) {
      const g = computeGauge(this._series[activeReqName]);
      const label = activeReqName.includes('cpu') ? 'CPU usage' : 'Active requests';
      const val   = activeReqName.includes('cpu') ? (g.last > 1 ? g.last : g.last * 100).toFixed(1) : fmtK(g.last);
      const unit  = activeReqName.includes('cpu') ? '%' : '';
      cards[5] = { ...cards[5], label, unit, value: val, sparkline: g.sparkline, change: +Math.abs(g.change).toFixed(1), trendUp: g.change > 0, changePositive: g.change >= 0 };
    }

    this.statCards.set(cards);
  }

  private buildRecentRows(names: string[]) {
    const rows: RecentMetricRow[] = names.slice(0, 100).map(name => {
      const seriesList = this._series[name] ?? [];
      const s    = seriesList[0];
      const pts  = s?.points ?? [];
      const last  = pts[pts.length - 1]?.value;
      const first = pts[0]?.value;
      const change = last != null && first != null && first > 0
        ? ((last - first) / first) * 100 : 0;
      const lastTs = pts.length ? tsToMs(pts[pts.length - 1].ts) : 0;
      return {
        name,
        kind:        s?.kind ?? '—',
        unit:        s?.unit ?? '',
        service:     s?.labels?.['service.name'] || s?.labels?.['service'] || '*',
        lastValue:   last != null ? fmtValue(last) : '—',
        lastTs,
        change:      +change.toFixed(1),
        trendUp:     change > 0,
        sparkline:   pts.slice(-12).map(p => p.value),
        cardinality: seriesList.length,
      };
    });
    this.recentRows.set(rows);
  }

  private buildTopServices() {
    const byService: Record<string, number> = {};
    for (const seriesList of Object.values(this._series)) {
      for (const s of seriesList) {
        let svc = s.labels?.['service.name'] || s.labels?.['service'] || '';
        if (!svc) {
          const route = s.labels?.['http.route'] || '';
          const m = route.match(/^\/?([^/]+)/);
          svc = m ? m[1] : 'other';
        }
        const last = s.points[s.points.length - 1];
        const cnt = last?.count ?? last?.value ?? 0;
        byService[svc] = (byService[svc] ?? 0) + cnt;
      }
    }
    const sorted = Object.entries(byService).sort((a, b) => b[1] - a[1]).slice(0, 6);
    const max = sorted[0]?.[1] || 1;
    this.topServices.set(sorted.map(([name, value]) => ({ name, value, pct: (value / max) * 100 })));
    this.allServices.set(sorted.map(([name]) => name));
  }

  private buildStatusPcts() {
    const s: Record<string, number> = { '2xx': 0, '3xx': 0, '4xx': 0, '5xx': 0 };
    for (const seriesList of Object.values(this._series)) {
      for (const series of seriesList) {
        const code = series.labels?.['http.status_code'] || series.labels?.['http.response.status_code'] || '';
        if (!code) continue;
        const last = series.points[series.points.length - 1];
        const cnt = last?.count ?? last?.value ?? 0;
        if      (code.startsWith('2')) s['2xx'] += cnt;
        else if (code.startsWith('3')) s['3xx'] += cnt;
        else if (code.startsWith('4')) s['4xx'] += cnt;
        else if (code.startsWith('5')) s['5xx'] += cnt;
      }
    }
    const total = Object.values(s).reduce((a, b) => a + b, 0) || 1;
    this.statusPcts.set(Object.fromEntries(
      Object.entries(s).map(([k, v]) => [k, ((v / total) * 100).toFixed(2)])
    ) as Record<string, string>);
  }

  // ── Chart rendering ───────────────────────────────────────────────────────
  private renderCharts() {
    this.destroyCharts();
    const names = this.allNames();

    const reqName = names.find(n => /http\.server\.request|request\.duration|http\.server/i.test(n));
    if (reqName && this._series[reqName]) {
      this.renderLineChart(this.reqRef, this._series[reqName], 'rate', COLORS_MAIN);
      this.renderLatencyChart(this.latRef, this._series[reqName]);
    }
    const errName = names.find(n => /error|exception/i.test(n));
    if (errName && this._series[errName]) {
      this.renderLineChart(this.errRef, this._series[errName], 'raw', ['#EF4444', '#f97316']);
    }
    const cpuName = names.find(n => /http\.server\.active_requests|cpu/i.test(n));
    if (cpuName && this._series[cpuName]) this.renderMiniChart(this.cpuRef, this._series[cpuName], '#38BDF8');

    const memName = names.find(n => /kestrel\.active_connections|memory|heap/i.test(n));
    if (memName && this._series[memName]) this.renderMiniChart(this.memRef, this._series[memName], '#a78bfa');

    this.renderDonut();
  }

  private renderLineChart(ref: ElementRef<HTMLCanvasElement> | undefined, series: MetricSeriesDto[], mode: 'rate' | 'raw', colors: string[]) {
    const canvas = ref?.nativeElement;
    if (!canvas || !series.length) return;
    const datasets = series.slice(0, 4).map((s, i) => {
      const color = colors[i % colors.length];
      const data = mode === 'rate' && s.points.length > 1
        ? s.points.slice(1).map((p, j) => {
            const dt = (p.ts - s.points[j].ts) / 1e9;
            const dv = p.value - s.points[j].value;
            return { x: tsToMs(p.ts), y: dt > 0 ? Math.max(0, dv / dt) : 0 };
          })
        : s.points.map(p => ({ x: tsToMs(p.ts), y: p.value }));
      const lbl = s.labels?.['service.name'] || s.labels?.['service'] || s.name;
      return { label: lbl, data, borderColor: color, backgroundColor: color + '18', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 1.5 };
    });
    this.charts.push(new Chart(canvas, { type: 'line', data: { datasets }, options: lineOpts() }));
  }

  private renderLatencyChart(ref: ElementRef<HTMLCanvasElement> | undefined, series: MetricSeriesDto[]) {
    const canvas = ref?.nativeElement;
    if (!canvas || !series.length) return;
    const main = series.filter(s => !s.labels?.['le']).slice(0, 3);
    const datasets = main.map((s, i) => {
      const scale = s.unit?.toLowerCase().includes('s') && !s.unit?.toLowerCase().includes('ms') ? 1000 : 1;
      const data = s.points.filter(p => p.value > 0)
        .map(p => ({ x: tsToMs(p.ts), y: p.value * scale }));
      const color = COLORS_LAT[i % COLORS_LAT.length];
      return { label: ['P50', 'P95', 'P99'][i], data, borderColor: color, backgroundColor: color + '18', fill: false, tension: 0.3, pointRadius: 0, borderWidth: 1.5 };
    });
    this.charts.push(new Chart(canvas, { type: 'line', data: { datasets }, options: lineOpts() }));
  }

  private renderMiniChart(ref: ElementRef<HTMLCanvasElement> | undefined, series: MetricSeriesDto[], color: string) {
    const canvas = ref?.nativeElement;
    if (!canvas || !series.length) return;
    const s = series[0];
    const data = s.points.map(p => ({ x: tsToMs(p.ts), y: p.value }));
    this.charts.push(new Chart(canvas, {
      type: 'line',
      data: { datasets: [{ data, borderColor: color, backgroundColor: color + '22', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 1.5 }] },
      options: miniOpts(),
    }));
  }

  private renderDonut() {
    const canvas = this.donutRef?.nativeElement;
    if (!canvas) return;
    const pcts = this.statusPcts();
    const data = [
      parseFloat(pcts['2xx']),
      parseFloat(pcts['3xx']),
      parseFloat(pcts['4xx']),
      parseFloat(pcts['5xx']),
    ];
    const total = data.reduce((a, b) => a + b, 0);
    this.charts.push(new Chart(canvas, {
      type: 'doughnut',
      data: {
        datasets: [{
          data:            total > 0 ? data : [1, 0, 0, 0],
          backgroundColor: total > 0
            ? ['#22C55E', '#F59E0B', '#f97316', '#EF4444']
            : ['#1e293b', '#1e293b', '#1e293b', '#1e293b'],
          borderWidth: 0,
          hoverOffset: 0,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        cutout: '68%',
        plugins: { legend: { display: false }, tooltip: { enabled: false } },
      },
    }));
  }

  private destroyCharts() {
    this.charts.forEach(c => c.destroy());
    this.charts = [];
  }

  // ── Template helpers ──────────────────────────────────────────────────────
  fmtChange(n: number): string { return Math.abs(n).toFixed(1) + '%'; }
  fmtSvcVal(v: number): string { return fmtK(v) + ' rps'; }

  onPreset(p: string) { this.preset.set(p); this.loadAll(); }

  private presetHours(): number {
    const m: Record<string, number> = { '15m': 0.25, '30m': 0.5, '1h': 1, '3h': 3, '6h': 6, '12h': 12, '24h': 24 };
    return m[this.preset()] ?? 1;
  }

  // ── Metrics list helpers ──────────────────────────────────────────────────
  toggleSort(col: string) {
    if (this.sortCol() === col) this.sortAsc.set(!this.sortAsc());
    else { this.sortCol.set(col); this.sortAsc.set(true); }
  }

  toggleGroup(ns: string) {
    const s = new Set(this.collapsedGroups());
    s.has(ns) ? s.delete(ns) : s.add(ns);
    this.collapsedGroups.set(s);
  }

  toggleRow(name: string) {
    const prev = this.expandedRow();
    if (prev === name) { this.expandedRow.set(null); return; }
    this.expandedRow.set(name);
    if (this._series[name]) {
      setTimeout(() => { this.renderInlineChart(name); this.cdr.markForCheck(); }, 0);
    } else {
      const from = formatISO(subHours(new Date(), this.presetHours()));
      this.api.queryMetric(name, from).pipe(catchError(() => of([] as MetricSeriesDto[]))).subscribe(r => {
        this._series[name] = r;
        this.cdr.detectChanges();
        setTimeout(() => { this.renderInlineChart(name); this.cdr.markForCheck(); }, 0);
      });
    }
  }

  private renderInlineChart(name: string) {
    const id  = this.getCanvasId(name);
    const old = this.inlineCharts.get(name);
    if (old) { old.destroy(); this.inlineCharts.delete(name); }
    const canvas = document.getElementById(id) as HTMLCanvasElement | null;
    if (!canvas) return;
    const series = this._series[name];
    if (!series?.length) return;
    const color = '#38BDF8';
    const data  = series[0].points.map(p => ({ x: tsToMs(p.ts), y: p.value }));
    this.inlineCharts.set(name, new Chart(canvas, {
      type: 'line',
      data: { datasets: [{ data, borderColor: color, backgroundColor: color + '18', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 1.5 }] },
      options: lineOpts(),
    }));
  }

  getCanvasId(name: string): string { return 'ic_' + name.replace(/[^a-zA-Z0-9]/g, '_'); }

  fmtLastSeen(tsMs: number): string {
    if (!tsMs) return '—';
    const diff = Date.now() - tsMs;
    if (diff < 60_000)    return Math.round(diff / 1_000)     + 's ago';
    if (diff < 3_600_000) return Math.round(diff / 60_000)   + 'm ago';
    return                     Math.round(diff / 3_600_000)  + 'h ago';
  }

  stalenessClass(tsMs: number): string {
    if (!tsMs) return 'stale';
    const diff = Date.now() - tsMs;
    if (diff > 1_800_000) return 'stale';
    if (diff > 300_000)   return 'warn';
    return 'fresh';
  }

  copyName(name: string, event: MouseEvent) {
    event.stopPropagation();
    navigator.clipboard.writeText(name).catch(() => {});
  }
}

// ── Constants ─────────────────────────────────────────────────────────────────
const COLORS_MAIN = ['#F59E0B', '#38BDF8', '#22C55E', '#f97316'];
const COLORS_LAT  = ['#38BDF8', '#a78bfa', '#22C55E'];

// ── Chart option factories ────────────────────────────────────────────────────
function lineOpts(): any {
  return {
    responsive: true, maintainAspectRatio: false, animation: false,
    interaction: { mode: 'index' as const, intersect: false },
    scales: {
      x: { type: 'linear', ticks: { color: '#64748b', maxTicksLimit: 5, font: { size: 10 }, callback: (v: any) => format(new Date(Number(v)), 'HH:mm') }, grid: { color: '#1e293b' }, border: { display: false } },
      y: { ticks: { color: '#64748b', font: { size: 10 }, callback: (v: any) => fmtValue(Number(v)) }, grid: { color: '#1e293b' }, border: { display: false } },
    },
    plugins: {
      legend: { display: false },
      tooltip: { backgroundColor: '#0f172a', borderColor: '#263244', borderWidth: 1, titleColor: '#e2e8f0', bodyColor: '#94a3b8', padding: 8 },
    },
  };
}

function miniOpts(): any {
  return {
    responsive: true, maintainAspectRatio: false, animation: false,
    scales: { x: { display: false, type: 'linear' }, y: { display: false } },
    plugins: { legend: { display: false }, tooltip: { enabled: false } },
  };
}

// ── Stat computation helpers ──────────────────────────────────────────────────
function aggregateRate(series: MetricSeriesDto[]): { rate: number; change: number; sparkline: number[] } {
  if (!series.length) return { rate: 0, change: 0, sparkline: [] };
  const merged: Record<number, number> = {};
  for (const s of series) for (const p of s.points) merged[p.ts] = (merged[p.ts] ?? 0) + p.value;
  const pts = Object.entries(merged).map(([ts, v]) => ({ ts: Number(ts), v })).sort((a, b) => a.ts - b.ts);
  if (pts.length < 2) return { rate: pts[0]?.v ?? 0, change: 0, sparkline: [] };
  const rates: number[] = [];
  for (let i = 1; i < pts.length; i++) {
    const dt = (pts[i].ts - pts[i - 1].ts) / 1e9;
    const dv = pts[i].v - pts[i - 1].v;
    if (dt > 0) rates.push(Math.max(0, dv / dt));
  }
  const half = Math.floor(rates.length / 2);
  const prevAvg = rates.slice(0, half).reduce((a, b) => a + b, 0) / (half || 1);
  const currAvg = rates.slice(half).reduce((a, b) => a + b, 0) / ((rates.length - half) || 1);
  return { rate: rates[rates.length - 1] ?? 0, change: prevAvg > 0 ? ((currAvg - prevAvg) / prevAvg) * 100 : 0, sparkline: rates };
}

function computeLatency(series: MetricSeriesDto[]): { p50: number; p95: number; change: number; sparkline: number[] } {
  const main = series.filter(s => !s.labels?.['le']);
  const src = main.length ? main : series.slice(0, 1);
  const avgs: number[] = [];
  for (const s of src) {
    const scale = s.unit?.toLowerCase().includes('s') && !s.unit?.toLowerCase().includes('ms') ? 1000 : 1;
    for (const p of s.points) if (p.value > 0) avgs.push(p.value * scale);
  }
  if (!avgs.length) return { p50: 0, p95: 0, change: 0, sparkline: [] };
  const sorted = [...avgs].sort((a, b) => a - b);
  const half = Math.floor(avgs.length / 2);
  const prevAvg = avgs.slice(0, half).reduce((a, b) => a + b, 0) / (half || 1);
  const currAvg = avgs.slice(half).reduce((a, b) => a + b, 0) / ((avgs.length - half) || 1);
  return {
    p50: sorted[Math.floor(sorted.length * 0.5)] ?? 0,
    p95: sorted[Math.floor(sorted.length * 0.95)] ?? 0,
    change: prevAvg > 0 ? ((currAvg - prevAvg) / prevAvg) * 100 : 0,
    sparkline: avgs,
  };
}

function computeGauge(series: MetricSeriesDto[]): { last: number; change: number; sparkline: number[] } {
  const s = series[0];
  if (!s?.points.length) return { last: 0, change: 0, sparkline: [] };
  const vals = s.points.map(p => p.value);
  const half = Math.floor(vals.length / 2);
  const prevAvg = vals.slice(0, half).reduce((a, b) => a + b, 0) / (half || 1);
  const currAvg = vals.slice(half).reduce((a, b) => a + b, 0) / ((vals.length - half) || 1);
  return { last: vals[vals.length - 1], change: prevAvg > 0 ? ((currAvg - prevAvg) / prevAvg) * 100 : 0, sparkline: vals };
}

// ── Value formatting ──────────────────────────────────────────────────────────
function tsToMs(ns: number): number { return Math.round(ns / 1_000_000); }
function fmtK(v: number): string {
  if (v >= 1_000_000) return (v / 1_000_000).toFixed(2) + 'M';
  if (v >= 1_000)     return (v / 1_000).toFixed(2) + 'k';
  return Number.isInteger(v) ? String(v) : v.toFixed(2);
}
function fmtValue(v: number): string {
  if (Math.abs(v) >= 1_000_000) return `${(v / 1_000_000).toFixed(2)}M`;
  if (Math.abs(v) >= 1_000)     return `${(v / 1_000).toFixed(2)}K`;
  return Number.isInteger(v) ? String(v) : v.toPrecision(4);
}

import {
  Component, signal, computed, inject, OnInit, OnDestroy,
  ChangeDetectionStrategy, ChangeDetectorRef, ViewChild, ElementRef,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import { Chart, registerables } from 'chart.js';
import zoomPlugin from 'chartjs-plugin-zoom';
import { format, subHours, formatISO } from 'date-fns';
import { ApiService } from '../../core/services/api.service';
import { DiagnosticsDto } from '../../core/models/diagnostics.model';
import { EventCountsDto } from '../../core/models/event.model';
import { fmtBytes, fmtNum, fmtUptime, fmtStartedAt } from '../../shared/utils/format';
import { SectionComponent } from '../../shared/components/ui';

Chart.register(...registerables, zoomPlugin);

type Tab = 'log-events' | 'diagnostics';

interface SvcRow { service: string; count: number; pct: number; }

const PALETTE = ['#38BDF8', '#F59E0B', '#22C55E', '#a78bfa', '#f97316',
                 '#ec4899', '#06b6d4', '#84cc16', '#eab308', '#8b5cf6'];
const PRESETS = ['1h', '3h', '6h', '12h', '24h', '7d'];

@Component({
  selector: 'app-diagnostics',
  imports: [FormsModule, LucideAngularModule, SectionComponent],
  templateUrl: './diagnostics.html',
  styleUrl: './diagnostics.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class DiagnosticsComponent implements OnInit, OnDestroy {
  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);

  @ViewChild('countsCanvas') countsRef?: ElementRef<HTMLCanvasElement>;
  private chart?: Chart;

  // ── Page state ─────────────────────────────────────────────────────────────
  tab = signal<Tab>('log-events');
  readonly presets = PRESETS;

  // ── Log events tab ─────────────────────────────────────────────────────────
  preset = signal<string>('24h');
  services = signal<string[]>([]);
  selectedServices = signal<string[]>([]);
  counts = signal<EventCountsDto | null>(null);
  countsLoading = signal(false);
  countsError = signal<string | null>(null);
  countsRefreshedAt = signal('');

  /** Services actually plotted: the user's selection, or the top 8 by count. */
  displayedServices = computed(() => {
    const c = this.counts();
    if (!c) return [];
    const sel = this.selectedServices();
    if (sel.length) {
      const set = new Set(sel);
      return c.services.filter(s => set.has(s.service));
    }
    return c.services.slice(0, 8);
  });

  serviceRows = computed<SvcRow[]>(() => {
    const c = this.counts();
    if (!c) return [];
    const max = c.services[0]?.count || 1;
    return c.services.map(s => ({ service: s.service, count: s.count, pct: (s.count / max) * 100 }));
  });

  totalDisplayed = computed(() => this.displayedServices().reduce((a, s) => a + s.count, 0));

  /** Stable palette index for a service based on its rank in the full result. */
  private svcIndex(svc: string): number {
    const c = this.counts();
    if (!c) return 0;
    return Math.max(0, c.services.findIndex(s => s.service === svc));
  }
  svcColor(svc: string): string {
    return PALETTE[this.svcIndex(svc) % PALETTE.length];
  }

  // ── Diagnostics tab (vital signs) ──────────────────────────────────────────
  diagLoading = signal(true);
  data = signal<DiagnosticsDto | null>(null);
  refreshedAt = signal('');

  private _diagInterval?: ReturnType<typeof setInterval>;
  private _countsInterval?: ReturnType<typeof setInterval>;

  ngOnInit(): void {
    this.loadServices();
    this.loadCounts();
    this.loadDiagnostics();
    this._diagInterval  = setInterval(() => { if (this.tab() === 'diagnostics') this.loadDiagnostics(); }, 10_000);
    this._countsInterval = setInterval(() => { if (this.tab() === 'log-events') this.loadCounts(); }, 30_000);
  }

  ngOnDestroy(): void {
    this.chart?.destroy();
    if (this._diagInterval)  clearInterval(this._diagInterval);
    if (this._countsInterval) clearInterval(this._countsInterval);
  }

  setTab(t: Tab): void { this.tab.set(t); }

  // ── Time helpers ───────────────────────────────────────────────────────────
  private hours(): number {
    const m: Record<string, number> = { '1h': 1, '3h': 3, '6h': 6, '12h': 12, '24h': 24, '7d': 168 };
    return m[this.preset()] ?? 24;
  }
  private fromIso(): string { return formatISO(subHours(new Date(), this.hours())); }

  onPreset(p: string): void { this.preset.set(p); this.loadCounts(); }

  // ── Service filter (multi-select chips) ────────────────────────────────────
  loadServices(): void {
    this.api.getServiceNames(7).subscribe({
      next: s => { this.services.set(s); this.cdr.markForCheck(); },
      error: () => { /* keep empty */ },
    });
  }

  onAddService(e: Event): void {
    const sel = e.target as HTMLSelectElement;
    const v = sel.value;
    sel.value = '';
    if (v) this.toggleService(v);
  }
  toggleService(svc: string): void {
    this.selectedServices.update(l => l.includes(svc) ? l.filter(s => s !== svc) : [...l, svc]);
    this.cdr.detectChanges();
    setTimeout(() => { this.renderChart(); this.cdr.markForCheck(); }, 0);
  }
  removeService(svc: string): void { this.toggleService(svc); }
  clearServices(): void {
    this.selectedServices.set([]);
    this.cdr.detectChanges();
    setTimeout(() => { this.renderChart(); this.cdr.markForCheck(); }, 0);
  }

  // ── Log events counts ──────────────────────────────────────────────────────
  loadCounts(): void {
    this.countsLoading.set(true);
    this.countsError.set(null);
    this.api.getEventCounts({ from: this.fromIso(), limit: 50_000 }).subscribe({
      next: d => {
        this.counts.set(d);
        this.countsLoading.set(false);
        this.countsRefreshedAt.set(new Date().toLocaleTimeString());
        this.cdr.detectChanges();
        setTimeout(() => { this.renderChart(); this.cdr.markForCheck(); }, 0);
      },
      error: () => {
        this.countsLoading.set(false);
        this.countsError.set('Failed to load event counts');
        this.cdr.markForCheck();
      },
    });
  }

  private renderChart(): void {
    this.chart?.destroy();
    this.chart = undefined;
    const canvas = this.countsRef?.nativeElement;
    const c = this.counts();
    if (!canvas || !c) return;

    const series = this.displayedServices();
    const datasets = series.map(s => {
      const color = this.svcColor(s.service);
      return {
        label: s.service,
        data: s.points.map((v, idx) => ({ x: c.buckets[idx], y: v })),
        borderColor: color,
        backgroundColor: color + '22',
        fill: series.length === 1,
        tension: 0.3,
        pointRadius: 0,
        borderWidth: 1.5,
      };
    });

    this.chart = new Chart(canvas, {
      type: 'line',
      data: { datasets },
      options: this.lineOpts(series.length > 1),
    });
  }

  private lineOpts(showLegend: boolean): any {
    const longRange = this.hours() >= 24;
    const tickFmt = (v: any) => format(new Date(Number(v)), longRange ? 'dd/MM HH:mm' : 'HH:mm');
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: 'index', intersect: false },
      scales: {
        x: {
          type: 'linear',
          ticks: { color: '#64748b', maxTicksLimit: 8, font: { size: 10 }, callback: tickFmt },
          grid: { color: '#1e293b' },
          border: { display: false },
        },
        y: {
          beginAtZero: true,
          ticks: { color: '#64748b', font: { size: 10 } },
          grid: { color: '#1e293b' },
          border: { display: false },
        },
      },
      plugins: {
        legend: { display: showLegend, labels: { color: '#94a3b8', font: { size: 10 }, boxWidth: 10, boxHeight: 10 } },
        tooltip: {
          backgroundColor: '#0f172a', borderColor: '#263244', borderWidth: 1,
          titleColor: '#e2e8f0', bodyColor: '#94a3b8', padding: 8,
          callbacks: { title: (items: any) => format(new Date(Number(items[0].parsed.x)), longRange ? 'dd/MM HH:mm' : 'HH:mm') },
        },
        zoom: {
          pan: { enabled: true, mode: 'x', modifierKey: 'shift' },
          zoom: {
            drag: { enabled: true, backgroundColor: 'rgba(56,189,248,0.15)', borderColor: '#38BDF8', borderWidth: 1 },
            wheel: { enabled: true },
            mode: 'x',
          },
        },
      },
    };
  }

  // ── Diagnostics (vital signs) ──────────────────────────────────────────────
  loadDiagnostics(): void {
    this.api.getDiagnostics().subscribe({
      next: d => {
        this.data.set(d);
        this.diagLoading.set(false);
        this.refreshedAt.set(new Date().toLocaleTimeString());
        this.cdr.markForCheck();
      },
      error: () => { this.diagLoading.set(false); this.cdr.markForCheck(); },
    });
  }

  readonly fmtBytes     = fmtBytes;
  readonly fmtNum       = fmtNum;
  readonly fmtUptime    = fmtUptime;
  readonly fmtStartedAt = fmtStartedAt;

  diskPercent(free: number, total: number): number {
    if (total === 0) return 0;
    return Math.round(((total - free) / total) * 100);
  }

  ramClass(pct: number, target: number): string {
    if (pct >= target)         return 'danger';
    if (pct >= target - 10)    return 'warning';
    return 'ok';
  }
}

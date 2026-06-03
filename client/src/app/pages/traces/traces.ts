import {
  Component, signal, computed, inject, OnInit, OnDestroy,
  ChangeDetectionStrategy, ChangeDetectorRef,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import { format } from 'date-fns';
import { ApiService } from '../../core/services/api.service';
import { SpanDto, TraceRowDto, TraceStatsDto } from '../../core/models/span.model';
import { subHours, formatISO } from 'date-fns';

@Component({
  selector: 'app-traces',
  imports: [FormsModule, LucideAngularModule],
  templateUrl: './traces.html',
  styleUrl: './traces.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class TracesComponent implements OnInit, OnDestroy {
  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);

  // ── Loading / data ────────────────────────────────────────────────────────
  statsLoading  = signal(false);
  stats         = signal<TraceStatsDto | null>(null);
  loading       = signal(false);
  traces        = signal<TraceRowDto[]>([]);

  selectedTraceId = signal<string | null>(null);
  traceSpans      = signal<SpanDto[]>([]);
  traceLoading    = signal(false);
  selectedSpan    = signal<SpanDto | null>(null);

  activeTraceTab: 'timeline' | 'details' = 'timeline';
  activeSpanTab:  'tags'                 = 'tags';

  // ── Filters ───────────────────────────────────────────────────────────────
  traceIdInput       = '';
  filterName         = '';
  filterService      = '';
  filterStatus       = '';
  filterMinDurationMs: number | null = null;
  filterMaxDurationMs: number | null = null;
  filterHttpStatus   = '';
  preset             = '1h';

  private _poll: ReturnType<typeof setInterval> | null = null;

  // ── Computed ──────────────────────────────────────────────────────────────
  services = computed(() => {
    const set = new Set(this.traces().map(t => t.serviceName).filter(Boolean));
    return [...set].sort();
  });

  filteredTraces = computed(() => this.traces());

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
    this.loadAll();
    this._poll = setInterval(() => this.loadAll(), 15_000);
  }

  ngOnDestroy() {
    if (this._poll) clearInterval(this._poll);
  }

  // ── Data loading ──────────────────────────────────────────────────────────
  loadAll() {
    const from = this.fromIso();
    this.loadStats(from);
    this.loadTraces(from);
  }

  loadStats(from: string) {
    this.statsLoading.set(true);
    this.api.getTraceStats(from).subscribe({
      next: s  => { this.stats.set(s); this.statsLoading.set(false); this.cdr.markForCheck(); },
      error: () => { this.statsLoading.set(false); this.cdr.markForCheck(); },
    });
  }

  loadTraces(from: string) {
    this.loading.set(true);
    this.api.searchTraces({
      from,
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

  applyFilters() { this.loadAll(); }

  jumpToTrace() {
    const id = this.traceIdInput.trim();
    if (!id) return;
    this.openTrace(id);
  }

  openTrace(traceId: string) {
    if (this.selectedTraceId() === traceId) return;
    this.selectedTraceId.set(traceId);
    this.selectedSpan.set(null);
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
  }

  selectSpan(span: SpanDto) {
    this.selectedSpan.set(this.selectedSpan()?.spanId === span.spanId ? null : span);
  }

  private fromIso(): string {
    const map: Record<string, number> = {
      '15m': 0.25, '30m': 0.5, '1h': 1, '3h': 3, '6h': 6, '12h': 12, '24h': 24,
    };
    return formatISO(subHours(new Date(), map[this.preset] ?? 1));
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

  svcColor(name: string): string {
    const PALETTE = ['#38BDF8', '#F59E0B', '#22C55E', '#a78bfa', '#f97316', '#ec4899', '#06b6d4', '#84cc16'];
    let h = 0;
    for (const c of name) h = (h * 31 + c.charCodeAt(0)) & 0x7fff_ffff;
    return PALETTE[h % PALETTE.length];
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
}

function isZeroId(id: string): boolean {
  return !id || /^0+$/.test(id);
}

function fmtMs(ms: number): string {
  if (ms < 0.001) return `${(ms * 1_000_000).toFixed(0)}ns`;
  if (ms < 1)     return `${(ms * 1_000).toFixed(0)}µs`;
  if (ms < 1_000) return `${ms.toFixed(2)}ms`;
  return `${(ms / 1_000).toFixed(3)}s`;
}

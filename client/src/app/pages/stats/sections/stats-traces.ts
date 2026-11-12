import { ChangeDetectionStrategy, Component, computed, inject, input, signal } from '@angular/core';
import { Router } from '@angular/router';
import { toObservable, toSignal } from '@angular/core/rxjs-interop';
import { catchError, forkJoin, map, of, startWith, switchMap } from 'rxjs';
import { format } from 'date-fns';

import { ApiService } from '../../../core/services/api.service';
import { LatencyServiceDto, TraceStatsDto } from '../../../core/models/span.model';
import { serviceColor } from '../../../shared/utils/service-color';
import {
  BreakdownTab, ChartBar, LegendItem, SectionFailure, SectionState, StatTile, StatsSectionComponent, sectionFailure,
} from '../components/stats-section/stats-section';
import { BreakdownHeader, BreakdownRow, StatsRowsComponent } from '../components/stats-rows/stats-rows';
import {
  SIGNAL_ROUTE, StatsWindow, axisTicks, bucketLinkParams, fmtBucket, pageLinkParams,
} from '../stats-range.util';
import { fmtBytes, fmtCount, fmtDateMs, fmtMs, fmtPct, fmtRate } from '../stats-format.util';

/** How many services the breakdown lists: what a band's height holds. */
const TOP_ROWS = 8;
/** An error share at or past this reads red in the services list. */
const ERROR_SHARE_HOT = 2;
/**
 * Histogram buckets from this index up are drawn in the warning colour: bucket 9 is the first past
 * the 1 s bound (bucket i spans `boundsMs[i-1]` – `boundsMs[i]`; `boundsMs[8]` is 1 000 ms).
 */
const SLOW_BUCKET = 9;

interface TracesResult {
  stats: TraceStatsDto | null;
  failure: SectionFailure | null;
  latency: LatencyServiceDto[];
}

interface HistogramBar { heightPct: number; color: string; title: string; }

/**
 * The Traces band: `GET /api/traces/stats?store=true` with the page's bucket count (the sparklines
 * then share the Logs chart's grid), its merged duration histogram and store counters, plus
 * `GET /api/traces/latency` for the per-service rows. Both answer from the `.stats` and
 * `.tracesum` sidecars — three reads per segment in the window, no span read — so a quarter costs
 * its segment count. The chart stacks the error traces, red, under the rest of each bucket.
 */
@Component({
  selector: 'app-stats-traces',
  imports: [StatsSectionComponent, StatsRowsComponent],
  template: `
    <app-stats-section signal="traces" [caption]="caption()" [tiles]="tiles()" [state]="state()"
      chartTitle="Traces" [chartCaption]="chartCaption()" [legend]="legend" [bars]="bars()" [ticks]="ticks()"
      emptyText="No traces in this window" [emptyHint]="emptyHint()" emptyRowsText="No service data."
      [errorText]="error()" [refused]="refused()" [tabs]="tabs" [(tab)]="tab" [breakdownNote]="note()" [openParams]="openParams()"
      (bucketOpen)="openBucket($event)">
      @if (tab() === 'latency') {
        <div class="lat">
          <div class="lat-figures tn">
            @for (f of latencyFigures(); track f.label) {
              <span class="lat-fig"><span class="lat-k">{{ f.label }}</span><span class="lat-v">{{ f.value }}</span></span>
            }
          </div>
          <div class="lat-bars" role="img" aria-label="Spans by duration">
            @for (b of histogram(); track $index) {
              <span class="lat-col" [title]="b.title"><span class="lat-fill" [style.height.%]="b.heightPct" [style.background]="b.color"></span></span>
            }
          </div>
          <div class="lat-ax tn">
            @for (t of histogramTicks; track t.label) { <span [style.left.%]="t.pct">{{ t.label }}</span> }
          </div>
        </div>
      } @else {
        <app-stats-rows [rows]="serviceRows()" [header]="serviceHeader" />
      }
    </app-stats-section>
  `,
  styles: `
    .lat { display: flex; flex-direction: column; flex: 1; min-height: 0; }
    .lat-figures { display: flex; gap: 14px; margin-bottom: 8px; }
    .lat-fig { display: flex; flex-direction: column; }
    .lat-k { font-size: var(--fs-2xs); color: var(--txt-dim); text-transform: uppercase; letter-spacing: var(--ls-caps); }
    .lat-v { font-size: var(--fs-md); font-weight: var(--fw-semibold); color: var(--txt); }
    .lat-bars { display: flex; align-items: flex-end; gap: 2px; flex: 1; min-height: 90px; }
    .lat-col { flex: 1 1 0; min-width: 0; height: 100%; display: flex; align-items: flex-end; }
    .lat-fill { width: 100%; min-height: 1px; border-radius: 2px 2px 0 0; opacity: .9; }
    .lat-ax { position: relative; height: 16px; margin-top: 6px; border-top: 1px solid var(--border); font-size: var(--fs-3xs); color: var(--txt-dim); flex-shrink: 0; }
    .lat-ax span { position: absolute; top: 3px; transform: translateX(-50%); white-space: nowrap; }
    .tn { font-variant-numeric: tabular-nums; }
  `,
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class StatsTracesSectionComponent {
  readonly window = input.required<StatsWindow>();
  readonly caption = input.required<string>();

  private readonly api = inject(ApiService);
  private readonly router = inject(Router);

  readonly tabs: BreakdownTab[] = [{ id: 'services', label: 'Services' }, { id: 'latency', label: 'Latency' }];
  readonly tab = signal('services');
  readonly serviceHeader: BreakdownHeader = { name: 'Service', bar: 'Spans', cols: ['P95', 'Err'] };
  readonly legend: LegendItem[] = [{ color: 'var(--info)', label: 'traces' }, { color: 'var(--error)', label: 'errors' }];
  /** The histogram's bounds worth a label: the right edge of the bucket each bound closes. */
  readonly histogramTicks = [
    { pct: (1 / 19) * 100, label: '1 ms' }, { pct: (3 / 19) * 100, label: '10 ms' }, { pct: (6 / 19) * 100, label: '100 ms' },
    { pct: (9 / 19) * 100, label: '1 s' }, { pct: (12 / 19) * 100, label: '10 s' }, { pct: (14 / 19) * 100, label: '1 min' },
    { pct: (17 / 19) * 100, label: '10 min' },
  ];

  private readonly result = toSignal(
    toObservable(this.window).pipe(
      switchMap(w => {
        // The whole grid, so the server's equal columns ARE the page's buckets; a closed custom
        // window's last column therefore runs to its bucket's end, a step at most past the end asked.
        const from = new Date(w.gridFromMs).toISOString();
        const to = new Date(w.gridEndMs).toISOString();
        return forkJoin({
          stats: this.api.getTraceStats(from, to, w.buckets, true).pipe(
            map(data => ({ data, failure: null as SectionFailure | null })),
            catchError(err => of({
              data: null as TraceStatsDto | null,
              failure: sectionFailure(err, 'Could not load the trace statistics.'),
            })),
          ),
          latency: this.api.getLatency(from, to).pipe(catchError(() => of([] as LatencyServiceDto[]))),
        }).pipe(
          map(({ stats, latency }): TracesResult => ({ stats: stats.data, failure: stats.failure, latency })),
          startWith(null),
        );
      }),
    ),
    { initialValue: null as TracesResult | null },
  );

  private readonly stats = computed(() => this.result()?.stats ?? null);
  readonly error = computed(() => this.result()?.failure?.error ?? '');
  readonly refused = computed(() => this.result()?.failure?.refused ?? false);

  readonly state = computed<SectionState>(() => {
    const r = this.result();
    if (!r) return 'loading';
    if (r.failure || !r.stats) return 'unavailable';
    return r.stats.totalTraces > 0 ? 'ready' : 'empty';
  });

  readonly tiles = computed<StatTile[]>(() => {
    const s = this.stats();
    const store = s?.store ?? null;
    const has = !!s?.totalTraces;
    return [
      { label: 'Traces', value: fmtCount(s?.totalTraces ?? 0), muted: !has },
      { label: 'Spans stored', value: fmtCount(store?.spans), muted: !store },
      { label: 'Error rate', value: has ? fmtPct(s!.errorRate) : '—', muted: !has },
      { label: 'Segments', value: fmtCount(store?.segments), muted: !store },
      { label: 'P95 latency', value: has ? fmtMs(s!.p95LatencyMs) : '—', muted: !has },
      { label: 'On disk', value: fmtBytes(store?.fileBytes), muted: !store },
      { label: 'Throughput', value: has ? fmtRate(s!.throughputRps, ' rps') : '—', muted: !has },
      { label: 'Services', value: fmtCount(store?.services), muted: !store },
    ];
  });

  readonly bars = computed<ChartBar[]>(() => {
    const s = this.stats();
    if (!s || s.totalSparkline.length === 0) return [];
    const n = s.totalSparkline.length;
    const fromMs = Date.parse(s.from);
    const toMs = Date.parse(s.to);
    const stepMs = (toMs - fromMs) / n;
    const max = Math.max(0, ...s.totalSparkline);
    return s.totalSparkline.map((total, i) => {
      const errors = s.errorSparkline[i] ?? 0;
      const startMs = fromMs + i * stepMs;
      const segments = [];
      if (total - errors > 0) segments.push({ color: 'var(--info)', weight: total - errors });
      if (errors > 0) segments.push({ color: 'var(--error)', weight: errors });
      return {
        startMs, endMs: startMs + stepMs, total, segments,
        heightPct: max > 0 && total > 0 ? Math.max(1.5, (total / max) * 100) : 0,
        title: `${format(startMs, 'd MMM HH:mm')} – ${format(startMs + stepMs, 'HH:mm')}\n${total.toLocaleString()} traces, ${errors.toLocaleString()} with errors · click to open`,
      };
    });
  });

  readonly ticks = computed(() => {
    const s = this.stats();
    if (!s) return [];
    return axisTicks(Date.parse(s.from), Date.parse(s.to));
  });

  readonly chartCaption = computed(() => {
    const s = this.stats();
    if (!s) return '';
    return `${s.totalSparkline.length} × ${fmtBucket(this.window().bucketSeconds)} · p50 ${fmtMs(s.p50LatencyMs)} · p99 ${fmtMs(s.p99LatencyMs)}`;
  });

  readonly note = computed(() => {
    const s = this.stats();
    return s ? `${fmtCount(s.serviceCount)} services · ${fmtCount(s.spanCount)} spans` : '';
  });

  readonly emptyHint = computed(() => {
    const store = this.stats()?.store;
    if (!store || store.oldestStartMs === null) return 'The trace store holds no segments yet.';
    return `The store's oldest trace is from ${fmtDateMs(store.oldestStartMs)}.`;
  });

  readonly serviceRows = computed<BreakdownRow[]>(() => {
    const ranked = [...(this.result()?.latency ?? [])].sort((a, b) => b.spanCount - a.spanCount).slice(0, TOP_ROWS);
    const top = ranked[0]?.spanCount || 1;
    return ranked.map(l => {
      const share = l.spanCount > 0 ? (l.errorCount / l.spanCount) * 100 : 0;
      return {
        key: l.service,
        name: l.service,
        color: serviceColor(l.service),
        barPct: (l.spanCount / top) * 100,
        cols: [
          { text: fmtMs(l.p95Ms) },
          { text: fmtPct(share), muted: share < ERROR_SHARE_HOT, color: share >= ERROR_SHARE_HOT ? 'var(--error)' : undefined },
        ],
      };
    });
  });

  readonly latencyFigures = computed(() => {
    const s = this.stats();
    return s ? [
      { label: 'p50', value: fmtMs(s.p50LatencyMs) }, { label: 'p95', value: fmtMs(s.p95LatencyMs) },
      { label: 'p99', value: fmtMs(s.p99LatencyMs) }, { label: 'max', value: fmtMs(s.maxLatencyMs) },
    ] : [];
  });

  readonly histogram = computed<HistogramBar[]>(() => {
    const h = this.stats()?.histogram;
    if (!h) return [];
    const max = Math.max(1, ...h.counts);
    return h.counts.map((count, i) => {
      const lower = i === 0 ? 0 : h.boundsMs[i - 1];
      const upper = h.boundsMs[i];
      return {
        heightPct: count > 0 ? Math.max(1.5, (count / max) * 100) : 0,
        color: i >= SLOW_BUCKET ? 'var(--warning)' : 'var(--info)',
        title: `${fmtMs(lower)} – ${upper === undefined ? '∞' : fmtMs(upper)}: ${count.toLocaleString()} spans`,
      };
    });
  });

  readonly openParams = computed(() => pageLinkParams('traces', this.window()));

  openBucket(bar: ChartBar): void {
    this.router.navigate([SIGNAL_ROUTE.traces], { queryParams: bucketLinkParams('traces', bar.startMs, bar.endMs) });
  }
}

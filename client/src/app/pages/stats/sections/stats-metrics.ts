import { ChangeDetectionStrategy, Component, computed, inject, input, signal } from '@angular/core';
import { Router } from '@angular/router';
import { toObservable, toSignal } from '@angular/core/rxjs-interop';
import { catchError, map, of, startWith, switchMap } from 'rxjs';
import { format } from 'date-fns';

import { ApiService } from '../../../core/services/api.service';
import { MetricStatsDto } from '../../../core/models/metric.model';
import {
  BreakdownTab, ChartBar, ChartSegment, SectionFailure, SectionState, StatTile, StatsSectionComponent, sectionFailure,
} from '../components/stats-section/stats-section';
import { BreakdownHeader, BreakdownRow, StatsRowsComponent } from '../components/stats-rows/stats-rows';
import {
  SIGNAL_ROUTE, StatsWindow, axisTicks, bucketLinkParams, fmtBucket, pageLinkParams, requestBounds,
} from '../stats-range.util';
import { fmtBytes, fmtCount, fmtDateMs, fmtPct } from '../stats-format.util';

/** The Metrics page's series palette, by rank: the chart's columns and the rows' badges share it. */
const PALETTE = ['#38BDF8', '#F59E0B', '#22C55E', '#A78BFA', '#F97316', '#EC4899', '#06B6D4', '#84CC16', '#EAB308', '#F43F5E'];
const OTHER_COLOR = 'var(--txt-dim)';
/**
 * How many metrics keep a colour and a row; the server answers ten, and the ones past this fold
 * into "other" here, so the chart's colours are exactly the rows' — seven of them and "other"
 * make the eight rows a band's height holds.
 */
const TOP_METRICS = 7;

const KIND_LETTER: Readonly<Record<string, string>> = { Counter: 'C', Gauge: 'G', Histogram: 'H' };
const KIND_COLOR: Readonly<Record<string, string>> = { Counter: 'var(--info)', Gauge: 'var(--success)', Histogram: '#A78BFA' };

interface MetricsResult { data: MetricStatsDto | null; failure: SectionFailure | null; }

/**
 * The Metrics band: `GET /api/metrics/stats` — the catalog by kind, the cold segments' bytes
 * spread over the window's buckets and attributed to their metrics, the store's totals and the
 * hot tier. No point is read for it, so three months cost what a day does. The chart is bytes on
 * disk per bucket of the time they cover (a segment's bytes spread over its range — not the bytes
 * written in that bucket, which the catalogue cannot tell), stacked by the biggest metrics; a
 * store refusal (#95) is the band's unavailable state with the server's sentence.
 */
@Component({
  selector: 'app-stats-metrics',
  imports: [StatsSectionComponent, StatsRowsComponent],
  template: `
    <app-stats-section signal="metrics" [caption]="caption()" [tiles]="tiles()" [state]="state()"
      chartTitle="On disk, by the time it covers" [chartCaption]="chartCaption()" [bars]="bars()" [ticks]="ticks()"
      emptyText="No metrics on disk for this window" [emptyHint]="emptyHint()" emptyRowsText="No metric data."
      [errorText]="error()" [refused]="refused()" [tabs]="tabs" [(tab)]="tab" [breakdownNote]="note()" [openParams]="openParams()"
      (bucketOpen)="openBucket($event)">
      @if (tab() === 'kinds') {
        <app-stats-rows [rows]="kindRows()" [header]="kindHeader" />
        <app-stats-rows [rows]="grainRows()" [header]="grainHeader" class="second" />
      } @else {
        <app-stats-rows [rows]="metricRows()" [header]="metricHeader" />
      }
    </app-stats-section>
  `,
  styles: `.second { margin-top: 6px; flex: none; }`,
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class StatsMetricsSectionComponent {
  readonly window = input.required<StatsWindow>();
  readonly caption = input.required<string>();

  private readonly api = inject(ApiService);
  private readonly router = inject(Router);

  readonly tabs: BreakdownTab[] = [{ id: 'top', label: 'Top metrics' }, { id: 'kinds', label: 'By kind' }];
  readonly tab = signal('top');
  readonly metricHeader: BreakdownHeader = { name: 'Metric', cols: ['Bytes', 'Series'] };
  readonly kindHeader: BreakdownHeader = { name: 'Kind', cols: ['Metrics', 'Series'] };
  readonly grainHeader: BreakdownHeader = { name: 'On disk', cols: ['Segments', 'Bytes'] };

  private readonly result = toSignal(
    toObservable(this.window).pipe(
      switchMap(w => {
        const bounds = requestBounds(w);
        return this.api.getMetricStats({ from: bounds.from, to: bounds.to, bucket: w.bucketSeconds }).pipe(
          map((data): MetricsResult => ({ data, failure: null })),
          catchError(err => of<MetricsResult>({ data: null, failure: sectionFailure(err, 'Could not load the metric statistics.') })),
          startWith(null),
        );
      }),
    ),
    { initialValue: null as MetricsResult | null },
  );

  private readonly data = computed(() => this.result()?.data ?? null);
  readonly error = computed(() => this.result()?.failure?.error ?? '');
  readonly refused = computed(() => this.result()?.failure?.refused ?? false);

  readonly state = computed<SectionState>(() => {
    const r = this.result();
    if (!r) return 'loading';
    if (r.failure || !r.data) return 'unavailable';
    return r.data.window.bytesWritten > 0 || r.data.window.metrics > 0 ? 'ready' : 'empty';
  });

  /** Bytes per bucket across every metric: the columns' heights. */
  private readonly bucketTotals = computed<number[]>(() => {
    const d = this.data();
    if (!d) return [];
    const totals = [...d.otherPoints];
    for (const m of d.byMetric) for (let i = 0; i < totals.length; i++) totals[i] += m.points[i] ?? 0;
    return totals;
  });

  readonly tiles = computed<StatTile[]>(() => {
    const d = this.data();
    const w = d?.window;
    const s = d?.store;
    const perHour = d && w ? w.peakBucketBytes / (d.bucketSeconds / 3600) : 0;
    const perDay = d?.bucketSeconds ? d.bucketSeconds >= 86_400 : false;
    const peak = w ? (perDay ? `${fmtBytes(w.peakBucketBytes)}/day` : `${fmtBytes(perHour)}/h`) : '—';
    const rawShare = w && w.bytesWritten > 0 ? fmtPct((w.rawBytes / w.bytesWritten) * 100, 0) : '—';
    return [
      { label: 'Metrics with data', value: fmtCount(w?.metrics ?? 0), muted: !w?.metrics },
      { label: 'Series', value: fmtCount(s?.series), muted: !s },
      { label: 'Stored', value: fmtBytes(w?.bytesWritten ?? 0), muted: !w?.bytesWritten },
      { label: 'Segments', value: fmtCount(s?.segments), muted: !s },
      { label: 'Densest bucket', value: w?.bytesWritten ? peak : '—', muted: !w?.bytesWritten },
      { label: 'On disk', value: fmtBytes(s?.bytes), muted: !s },
      { label: 'Raw points', value: rawShare, muted: !w?.bytesWritten },
      { label: 'Hot tier', value: s ? `${fmtCount(s.hotPoints)} pts` : '—', muted: !s },
    ];
  });

  readonly bars = computed<ChartBar[]>(() => {
    const d = this.data();
    if (!d || d.buckets.length === 0) return [];
    const totals = this.bucketTotals();
    const max = Math.max(0, ...totals);
    const stepMs = d.bucketSeconds * 1000;
    return d.buckets.map((startMs, i) => {
      const segments: ChartSegment[] = [];
      let other = d.otherPoints[i] ?? 0;
      d.byMetric.forEach((m, rank) => {
        const v = m.points[i] ?? 0;
        if (v <= 0) return;
        if (rank < TOP_METRICS) segments.push({ color: PALETTE[rank % PALETTE.length], weight: v });
        else other += v;
      });
      if (other > 0) segments.push({ color: OTHER_COLOR, weight: other });
      const total = totals[i];
      return {
        startMs, endMs: startMs + stepMs, total, segments,
        heightPct: max > 0 && total > 0 ? Math.max(1.5, (total / max) * 100) : 0,
        title: `${format(startMs, 'd MMM HH:mm')} – ${format(startMs + stepMs, 'HH:mm')}\n${fmtBytes(total)} on disk · click to open`,
      };
    });
  });

  readonly ticks = computed(() => {
    const d = this.data();
    if (!d || d.buckets.length === 0) return [];
    return axisTicks(d.buckets[0], d.buckets[d.buckets.length - 1] + d.bucketSeconds * 1000);
  });

  readonly chartCaption = computed(() => {
    const d = this.data();
    if (!d) return '';
    const w = d.window;
    const raw = w.bytesWritten > 0 ? Math.round((w.rawBytes / w.bytesWritten) * 100) : 0;
    return `${d.buckets.length} × ${fmtBucket(d.bucketSeconds)} · ${fmtBytes(w.bytesWritten)} · raw ${raw} % · rollups ${100 - raw} %`;
  });

  readonly note = computed(() => {
    const s = this.data()?.store;
    return s ? `${fmtCount(s.metrics)} metrics · ${fmtCount(s.series)} series` : '';
  });

  readonly emptyHint = computed(() => {
    const s = this.data()?.store;
    if (!s) return '';
    if (s.oldestMs === null) return 'The metric store holds no segments yet.';
    return `The store's oldest point is from ${fmtDateMs(s.oldestMs)}; the catalog knows ${fmtCount(s.metrics)} metrics.`;
  });

  readonly metricRows = computed<BreakdownRow[]>(() => {
    const d = this.data();
    if (!d) return [];
    const top = d.byMetric[0]?.bytes || 1;
    const rows: BreakdownRow[] = d.byMetric.slice(0, TOP_METRICS).map((m, rank) => ({
      key: m.name,
      name: m.name,
      badge: KIND_LETTER[m.kind] ?? '?',
      badgeColor: PALETTE[rank % PALETTE.length],
      barPct: (m.bytes / top) * 100,
      cols: [{ text: fmtBytes(m.bytes) }, { text: fmtCount(m.series), muted: true }],
    }));
    const other = d.otherBytes + d.byMetric.slice(TOP_METRICS).reduce((a, m) => a + m.bytes, 0);
    if (other > 0) {
      rows.push({
        // A key no metric name can have: `@for` tracks the rows by it, and a metric named "other" is legal.
        key: '\u0000other', name: 'other metrics', color: OTHER_COLOR,
        barPct: (other / top) * 100,
        cols: [{ text: fmtBytes(other) }, { text: '', muted: true }],
      });
    }
    return rows;
  });

  readonly kindRows = computed<BreakdownRow[]>(() => {
    const d = this.data();
    if (!d) return [];
    const top = Math.max(1, ...d.kinds.map(k => k.series));
    return d.kinds.map(k => ({
      key: k.kind,
      name: k.kind,
      badge: KIND_LETTER[k.kind] ?? '?',
      badgeColor: KIND_COLOR[k.kind] ?? OTHER_COLOR,
      barPct: (k.series / top) * 100,
      cols: [{ text: fmtCount(k.metrics) }, { text: fmtCount(k.series), muted: true }],
    }));
  });

  readonly grainRows = computed<BreakdownRow[]>(() => {
    const s = this.data()?.store;
    if (!s) return [];
    const top = Math.max(1, s.bytesRaw, s.bytes5m, s.bytes1h);
    const grain = (key: string, name: string, segments: number, bytes: number): BreakdownRow => ({
      key, name, color: OTHER_COLOR, barPct: (bytes / top) * 100,
      cols: [{ text: fmtCount(segments) }, { text: fmtBytes(bytes), muted: true }],
    });
    return [
      grain('raw', 'Raw points', s.segmentsRaw, s.bytesRaw),
      grain('5m', '5-minute rollups', s.segments5m, s.bytes5m),
      grain('1h', '1-hour rollups', s.segments1h, s.bytes1h),
    ];
  });

  readonly openParams = computed(() => pageLinkParams('metrics', this.window()));

  openBucket(bar: ChartBar): void {
    this.router.navigate([SIGNAL_ROUTE.metrics], { queryParams: bucketLinkParams('metrics', bar.startMs, bar.endMs) });
  }
}

import { ChangeDetectionStrategy, Component, computed, inject, input, signal } from '@angular/core';
import { Router } from '@angular/router';
import { toObservable, toSignal } from '@angular/core/rxjs-interop';
import { catchError, forkJoin, map, of, startWith, switchMap } from 'rxjs';
import { format } from 'date-fns';

import { ApiService } from '../../../core/services/api.service';
import { EventCountsDto, LEVELS, StatsDto } from '../../../core/models/event.model';
import { serviceColor } from '../../../shared/utils/service-color';
import {
  BreakdownTab, ChartBar, ChartSegment, SectionFailure, SectionState, StatTile, StatsSectionComponent, sectionFailure,
} from '../components/stats-section/stats-section';
import { BreakdownRow, StatsRowsComponent } from '../components/stats-rows/stats-rows';
import {
  SIGNAL_ROUTE, StatsWindow, axisTicks, bucketLinkParams, fmtBucket, pageLinkParams, requestBounds,
} from '../stats-range.util';
import { fmtBytes, fmtCount, fmtDateMs, fmtPct, fmtRate } from '../stats-format.util';

/** How many services get their own colour in the chart, and a row each, before the tail folds into "other". */
const TOP_SERVICES = 8;
/** How many rows the breakdown lists: what a band's height holds. */
const TOP_ROWS = 8;
/** The counts endpoint lists at most this many services; past it the count is a floor. */
const SERVICES_CAP = 25;

const LEVEL_COLORS: Readonly<Record<string, string>> = {
  Verbose: 'var(--lvl-v)', Debug: 'var(--lvl-d)', Information: 'var(--lvl-i)',
  Warning: 'var(--lvl-w)', Error: 'var(--lvl-e)', Fatal: 'var(--lvl-f)',
};

/** One answer of the band's two requests: the counts, or why there are none; the store's counters, or null. */
interface LogsResult {
  counts: EventCountsDto | null;
  failure: SectionFailure | null;
  stats: StatsDto | null;
}

/**
 * The Logs band: ingest volume over the window bucketed by service (`GET /api/events/counts`) and
 * the log store's counters (`GET /api/stats`). The counts are the one request of the page that is
 * not a catalogue read: segments wholly inside the window are counted from their per-segment
 * summaries (every preset's bucket is a whole number of 15-minute slots), and only the segments the
 * window's edges cut are header-scanned. It is cached 20 s by bucket grid, and past seven days runs
 * as a search — a slot and a budget — so on a busy server it may still come back refused (503) or
 * out of budget (504) with the server's sentence, most likely on the first wide window after a
 * restart, while segments written before the summaries existed are scanned once. That, or the store
 * closed (#95), is the band's unavailable state; the store counters failing alone only dashes the
 * store's tiles.
 */
@Component({
  selector: 'app-stats-logs',
  imports: [StatsSectionComponent, StatsRowsComponent],
  template: `
    <app-stats-section signal="logs" [caption]="caption()" [tiles]="tiles()" [state]="state()"
      chartTitle="Ingest volume" [chartCaption]="chartCaption()" [bars]="bars()" [ticks]="ticks()"
      emptyText="No events in this window" [emptyHint]="emptyHint()" emptyRowsText="No service data."
      [errorText]="error()" [refused]="refused()" [tabs]="tabs" [(tab)]="tab" [breakdownNote]="note()" [openParams]="openParams()"
      (bucketOpen)="openBucket($event)">
      @if (tab() === 'levels') {
        <app-stats-rows [rows]="levelRows()" />
      } @else {
        <app-stats-rows [rows]="serviceRows()" />
      }
    </app-stats-section>
  `,
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class StatsLogsSectionComponent {
  readonly window = input.required<StatsWindow>();
  readonly caption = input.required<string>();

  private readonly api = inject(ApiService);
  private readonly router = inject(Router);

  readonly tabs: BreakdownTab[] = [{ id: 'services', label: 'Top services' }, { id: 'levels', label: 'Levels' }];
  readonly tab = signal('services');

  /** Both requests per window; null while they are out. */
  private readonly result = toSignal(
    toObservable(this.window).pipe(
      switchMap(w => {
        const bounds = requestBounds(w);
        return forkJoin({
          counts: this.api.getEventCounts({ from: bounds.from, to: bounds.to, bucket: w.bucketSeconds }).pipe(
            map(data => ({ data, failure: null as SectionFailure | null })),
            catchError(err => of({
              data: null as EventCountsDto | null,
              failure: sectionFailure(err, 'Could not load the event counts.'),
            })),
          ),
          stats: this.api.getStats().pipe(catchError(() => of(null as StatsDto | null))),
        }).pipe(
          map(({ counts, stats }): LogsResult => ({ counts: counts.data, failure: counts.failure, stats })),
          startWith(null),
        );
      }),
    ),
    { initialValue: null as LogsResult | null },
  );

  private readonly counts = computed(() => this.result()?.counts ?? null);
  private readonly stats = computed(() => this.result()?.stats ?? null);
  readonly error = computed(() => this.result()?.failure?.error ?? '');
  readonly refused = computed(() => this.result()?.failure?.refused ?? false);

  readonly state = computed<SectionState>(() => {
    const r = this.result();
    if (!r) return 'loading';
    if (r.failure) return 'unavailable';
    return (r.counts?.total ?? 0) > 0 ? 'ready' : 'empty';
  });

  /** Events per bucket: the level series cover every event; the service series stop at the cap. */
  private readonly bucketTotals = computed<number[]>(() => {
    const c = this.counts();
    if (!c) return [];
    const totals = new Array<number>(c.buckets.length).fill(0);
    const series = c.levels?.length ? c.levels : c.services;
    for (const s of series) for (let i = 0; i < totals.length; i++) totals[i] += s.points[i] ?? 0;
    return totals;
  });

  private readonly peakBucket = computed(() => Math.max(0, ...this.bucketTotals()));

  private readonly errorRate = computed(() => {
    const c = this.counts();
    if (!c?.levels?.length || c.total === 0) return 0;
    const errors = c.levels.reduce((a, l) => a + (l.level === 'Error' || l.level === 'Fatal' ? l.count : 0), 0);
    return (errors / c.total) * 100;
  });

  private readonly oldestMs = computed(() => {
    const s = this.stats();
    return s?.oldestTimestamp ? Date.parse(s.oldestTimestamp) : null;
  });

  readonly tiles = computed<StatTile[]>(() => {
    const c = this.counts();
    const s = this.stats();
    const bs = c?.bucketSeconds ?? 0;
    const services = c?.services.length ?? 0;
    return [
      { label: 'Events', value: fmtCount(c?.total ?? 0), muted: !c?.total },
      { label: 'Stored events', value: fmtCount(s?.totalEvents), muted: !s },
      { label: 'Peak rate', value: fmtRate(bs ? this.peakBucket() / bs : 0), muted: !c?.total },
      { label: 'Segments', value: fmtCount(s?.segments), muted: !s },
      { label: 'Error rate', value: c?.total ? fmtPct(this.errorRate()) : '—', muted: !c?.total },
      { label: 'On disk', value: fmtBytes(s?.compressedBytes), muted: !s },
      { label: 'Services', value: services >= SERVICES_CAP ? `${SERVICES_CAP}+` : fmtCount(services), muted: !c?.total },
      { label: 'Oldest event', value: fmtDateMs(this.oldestMs()), muted: this.oldestMs() === null },
    ];
  });

  /** One stacked bar per bucket: the top services coloured, the rest a muted "other". */
  readonly bars = computed<ChartBar[]>(() => {
    const c = this.counts();
    if (!c || c.buckets.length === 0) return [];
    const totals = this.bucketTotals();
    const max = this.peakBucket();
    const top = [...c.services].sort((a, b) => b.count - a.count).slice(0, TOP_SERVICES);
    const stepMs = c.bucketSeconds * 1000;
    return c.buckets.map((startMs, i) => {
      const segments: ChartSegment[] = [];
      let known = 0;
      for (const s of top) {
        const v = s.points[i] ?? 0;
        if (v > 0) { segments.push({ color: serviceColor(s.service), weight: v }); known += v; }
      }
      const other = totals[i] - known;
      if (other > 0) segments.push({ color: 'var(--txt-dim)', weight: other });
      const total = totals[i];
      return {
        startMs, endMs: startMs + stepMs, total, segments,
        heightPct: max > 0 && total > 0 ? Math.max(1.5, (total / max) * 100) : 0,
        title: `${format(startMs, 'd MMM HH:mm')} – ${format(startMs + stepMs, 'HH:mm')}\n${total.toLocaleString()} events · click to open`,
      };
    });
  });

  readonly ticks = computed(() => {
    const c = this.counts();
    if (!c || c.buckets.length === 0) return [];
    return axisTicks(c.buckets[0], c.buckets[c.buckets.length - 1] + c.bucketSeconds * 1000);
  });

  readonly chartCaption = computed(() => {
    const c = this.counts();
    if (!c) return '';
    const bs = c.bucketSeconds;
    const active = this.bucketTotals().filter(t => t > 0).length;
    const avg = bs > 0 && active > 0 ? c.total / (active * bs) : 0;
    return `${c.buckets.length} × ${fmtBucket(bs)} · avg ${fmtRate(avg)} · peak ${fmtRate(bs ? this.peakBucket() / bs : 0)}`;
  });

  readonly note = computed(() => {
    const c = this.counts();
    if (!c) return '';
    const services = c.services.length;
    return `${services >= SERVICES_CAP ? `${SERVICES_CAP}+` : services} services · ${fmtPct(this.errorRate())} errors`;
  });

  readonly emptyHint = computed(() => {
    const oldest = this.oldestMs();
    return oldest === null ? 'The log store holds no segments yet.' : `The store's oldest event is from ${fmtDateMs(oldest)}.`;
  });

  readonly serviceRows = computed<BreakdownRow[]>(() => {
    const c = this.counts();
    if (!c) return [];
    const total = c.total || 1;
    const ranked = [...c.services].sort((a, b) => b.count - a.count).slice(0, TOP_ROWS);
    const top = ranked[0]?.count || 1;
    return ranked.map(s => ({
      key: s.service || '(no service)',
      name: s.service || '(no service)',
      color: serviceColor(s.service),
      barPct: (s.count / top) * 100,
      cols: [{ text: fmtCount(s.count) }, { text: fmtPct((s.count / total) * 100), muted: true }],
    }));
  });

  readonly levelRows = computed<BreakdownRow[]>(() => {
    const c = this.counts();
    const levels = c?.levels ?? [];
    if (!c || levels.length === 0) return [];
    const total = c.total || 1;
    const order = LEVELS as readonly string[];
    const top = Math.max(...levels.map(l => l.count), 1);
    return [...levels]
      .sort((a, b) => order.indexOf(a.level) - order.indexOf(b.level))
      .map(l => ({
        key: l.level,
        name: l.level,
        color: LEVEL_COLORS[l.level] ?? 'var(--lvl-v)',
        barPct: (l.count / top) * 100,
        cols: [{ text: fmtCount(l.count) }, { text: fmtPct((l.count / total) * 100), muted: true }],
      }));
  });

  readonly openParams = computed(() => pageLinkParams('logs', this.window()));

  /** A bar → the Logs page scoped to that bucket. */
  openBucket(bar: ChartBar): void {
    this.router.navigate([SIGNAL_ROUTE.logs], { queryParams: bucketLinkParams('logs', bar.startMs, bar.endMs) });
  }
}

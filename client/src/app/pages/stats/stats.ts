import { Component, computed, inject, signal, ChangeDetectionStrategy } from '@angular/core';
import { Router } from '@angular/router';
import { toSignal, toObservable } from '@angular/core/rxjs-interop';
import { of, switchMap, catchError } from 'rxjs';
import { format } from 'date-fns';
import { LucideAngularModule } from 'lucide-angular';

import { ApiService } from '../../core/services/api.service';
import { EventCountsDto, StatsDto, LEVELS } from '../../core/models/event.model';
import { serviceColor } from '../../shared/utils/service-color';
import { PageHeaderComponent, EmptyStateComponent } from '../../shared/components/ui';

type RangeKey = '1h' | '6h' | '24h' | '7d';

const RANGE_MS: Record<RangeKey, number> = {
  '1h':  3_600_000,
  '6h':  6  * 3_600_000,
  '24h': 24 * 3_600_000,
  '7d':  7  * 24 * 3_600_000,
};

/** Target bar count across the window; the server `bucket` (seconds) is derived to hit this. */
const TARGET_BUCKETS = 60;
/** How many top services get their own colour before the tail collapses into "other". */
const TOP_SERVICES = 10;

interface Seg { service: string; color: string; weight: number; }
interface Bar {
  index: number;
  startMs: number;
  endMs: number;
  total: number;
  /** Filled height 0–100 %, normalised to the tallest bucket in the window. */
  heightPct: number;
  segments: Seg[];
  title: string;
}
interface Tick { pct: number; label: string; }
interface SvcStat { service: string; color: string; count: number; pct: number; pctLabel: string; }
interface LevelRow { level: string; color: string; count: number; pct: number; pctLabel: string; }

/** Severity → colour for the per-level breakdown. */
const LEVEL_COLORS: Record<string, string> = {
  Verbose: '#64748b', Debug: '#38BDF8', Information: '#22C55E',
  Warning: '#F59E0B', Error: '#EF4444', Fatal: '#B91C1C',
};

/**
 * Statistics page: log ingest volume over a selectable window (stacked by service,
 * coloured via the shared {@link serviceColor}), a top-services breakdown, and
 * store-level counters from <c>/api/stats</c>. Self-contained — it fetches its own
 * data and owns its own time-range control, independent of the Events page store.
 */
@Component({
  selector: 'app-stats',
  imports: [LucideAngularModule, PageHeaderComponent, EmptyStateComponent],
  templateUrl: './stats.html',
  styleUrl: './stats.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class StatsComponent {
  private readonly api = inject(ApiService);
  private readonly router = inject(Router);

  readonly ranges: { key: RangeKey; label: string }[] = [
    { key: '1h',  label: '1h'  },
    { key: '6h',  label: '6h'  },
    { key: '24h', label: '24h' },
    { key: '7d',  label: '7d'  },
  ];
  readonly range = signal<RangeKey>('24h');

  /** `from` (ISO) + derived bucket size for the selected window. `to` is left
   *  open-ended so the server uses its own "now" — this avoids recent events
   *  being cut off by any browser/server clock skew (matches the events query). */
  private readonly params = computed(() => {
    const span = RANGE_MS[this.range()];
    return {
      from: new Date(Date.now() - span).toISOString(),
      bucket: Math.max(1, Math.round(span / 1000 / TARGET_BUCKETS)),
    };
  });

  /** Per-service counts bucketed over the window; refetched whenever the range changes. */
  private readonly counts = toSignal(
    toObservable(this.params).pipe(
      switchMap(p =>
        this.api.getEventCounts({ from: p.from, bucket: p.bucket })
          .pipe(catchError(() => of(null))),
      ),
    ),
    { initialValue: null as EventCountsDto | null },
  );

  /** Store-level counters (fetched once). */
  private readonly stats = toSignal(
    this.api.getStats().pipe(catchError(() => of(null))),
    { initialValue: null as StatsDto | null },
  );

  readonly loading = computed(() => this.counts() === null);
  readonly hasData = computed(() => this.totalEvents() > 0);

  readonly totalEvents  = computed(() => this.counts()?.total ?? 0);
  readonly serviceCount = computed(() => this.counts()?.services.length ?? 0);
  readonly truncated    = computed(() => this.counts()?.truncated ?? false);

  readonly segments        = computed(() => this.stats()?.segments ?? null);
  readonly storageEvents   = computed(() => this.stats()?.totalEvents ?? null);
  readonly compressedBytes = computed(() => this.stats()?.compressedBytes ?? null);

  /** One stacked bar per bucket; top services coloured, the rest merged into a muted "other". */
  readonly bars = computed<Bar[]>(() => {
    const data = this.counts();
    if (!data || data.buckets.length === 0) return [];

    const ranked = [...data.services].sort((a, b) => b.count - a.count);
    const top = ranked.slice(0, TOP_SERVICES);
    const rest = ranked.slice(TOP_SERVICES);
    const bucketMs = data.bucketSeconds * 1000;

    const bars: Bar[] = data.buckets.map((startMs, i) => {
      const segments: Seg[] = [];
      let total = 0;
      for (const s of top) {
        const v = s.points[i] ?? 0;
        if (v > 0) { segments.push({ service: s.service, color: serviceColor(s.service), weight: v }); total += v; }
      }
      let other = 0;
      for (const s of rest) other += s.points[i] ?? 0;
      if (other > 0) { segments.push({ service: 'other', color: 'var(--txt-muted)', weight: other }); total += other; }

      return {
        index: i, startMs, endMs: startMs + bucketMs, total, heightPct: 0, segments,
        title: `${format(startMs, 'MMM d, HH:mm')} – ${format(startMs + bucketMs, 'HH:mm')}\n${total.toLocaleString()} events · click to open`,
      };
    });

    const max = Math.max(...bars.map(b => b.total));
    if (max > 0) for (const b of bars) b.heightPct = b.total > 0 ? Math.max(1.5, (b.total / max) * 100) : 0;
    return bars;
  });

  readonly maxCount = computed(() => {
    const bars = this.bars();
    return bars.length ? Math.max(...bars.map(b => b.total)) : 0;
  });

  private readonly bucketSeconds = computed(() => this.counts()?.bucketSeconds ?? 0);

  /** Busiest bucket's rate — the ingest peak. */
  readonly peakRate = computed(() => {
    const bs = this.bucketSeconds();
    return bs > 0 ? this.maxCount() / bs : 0;
  });

  /** Average rate over the buckets that actually have data (avoids diluting by empty span). */
  readonly avgRate = computed(() => {
    const bs = this.bucketSeconds();
    const active = this.bars().filter(b => b.total > 0).length;
    return bs > 0 && active > 0 ? this.totalEvents() / (active * bs) : 0;
  });

  /** Evenly-spaced time labels under the chart. */
  readonly axisTicks = computed<Tick[]>(() => {
    const bars = this.bars();
    if (!bars.length) return [];
    const fmt = this.range() === '7d' ? 'MMM d' : 'HH:mm';
    const n = 5;
    return Array.from({ length: n }, (_, i) => {
      const idx = Math.round((i / (n - 1)) * (bars.length - 1));
      return { pct: (i / (n - 1)) * 100, label: format(bars[idx].startMs, fmt) };
    });
  });

  readonly topServices = computed<SvcStat[]>(() => {
    const data = this.counts();
    if (!data) return [];
    const total = data.total || data.services.reduce((s, x) => s + x.count, 0) || 1;
    return [...data.services]
      .sort((a, b) => b.count - a.count)
      .slice(0, TOP_SERVICES)
      .map(s => {
        const pct = (s.count / total) * 100;
        return { service: s.service, color: serviceColor(s.service), count: s.count, pct, pctLabel: `${pct.toFixed(pct < 10 ? 1 : 0)}%` };
      });
  });

  /** Per-level breakdown from the header-scan `levels` series (canonical severity order). */
  readonly levelRows = computed<LevelRow[]>(() => {
    const c = this.counts();
    const levels = c?.levels ?? [];
    if (!c || !levels.length) return [];
    const total = c.total || levels.reduce((a, l) => a + l.count, 0) || 1;
    const order = LEVELS as readonly string[];
    return [...levels]
      .sort((a, b) => order.indexOf(a.level) - order.indexOf(b.level))
      .map(l => {
        const pct = (l.count / total) * 100;
        return { level: l.level, color: LEVEL_COLORS[l.level] ?? '#64748b', count: l.count, pct, pctLabel: `${pct.toFixed(pct < 10 ? 1 : 0)}%` };
      });
  });

  /** Error+Fatal share of counted events. */
  readonly errorRate = computed(() => {
    const c = this.counts();
    if (!c?.levels?.length || c.total === 0) return 0;
    const errs = c.levels.reduce((a, l) => a + (l.level === 'Error' || l.level === 'Fatal' ? l.count : 0), 0);
    return (errs / c.total) * 100;
  });
  readonly errorRateLabel = computed(() => `${this.errorRate().toFixed(1)}%`);

  setRange(k: RangeKey): void { this.range.set(k); }

  /** Click a volume bar → open the Events page scoped to that bucket's time window. */
  openBucket(bar: Bar): void {
    this.router.navigate(['/events'], {
      queryParams: {
        preset: 'custom',
        from: format(bar.startMs, 'yyyy-MM-dd HH:mm'),
        to:   format(bar.endMs,   'yyyy-MM-dd HH:mm'),
      },
    });
  }

  fmtRate(n: number): string {
    if (n <= 0) return '0/s';
    return n < 10 ? `${n.toFixed(1)}/s` : `${Math.round(n).toLocaleString()}/s`;
  }

  fmtNum(n: number | null): string {
    return n == null ? '—' : n.toLocaleString();
  }

  fmtBytes(n: number | null): string {
    if (n == null) return '—';
    if (n < 1024) return `${n} B`;
    const units = ['KB', 'MB', 'GB', 'TB'];
    let v = n / 1024, i = 0;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
    return `${v.toFixed(v < 10 ? 1 : 0)} ${units[i]}`;
  }
}

import { ChangeDetectionStrategy, Component, computed, input, model, output } from '@angular/core';
import { Params, RouterLink } from '@angular/router';
import { LucideAngularModule } from 'lucide-angular';

import { SIGNAL_ROUTE, StatsSignal } from '../../stats-range.util';

/** One of the eight tiles: the window's four in the left column, the store's four in the right. */
export interface StatTile {
  label: string;
  value: string;
  /** A dash or a zero drawn quieter than a figure. */
  muted?: boolean;
}

export interface ChartSegment { color: string; weight: number; }

/** One column of a band's chart: a bucket of the window. */
export interface ChartBar {
  startMs: number;
  endMs: number;
  total: number;
  /** Filled height 0–100 %, against the tallest bucket in the window. */
  heightPct: number;
  segments: ChartSegment[];
  title: string;
}

export interface AxisTick { pct: number; label: string; }
export interface LegendItem { color: string; label: string; }
export interface BreakdownTab { id: string; label: string; }

/**
 * `loading` until the band's request answers; `unavailable` when the store refused (a 503 with its
 * own sentence) or the request failed; `empty` when it answered and the window holds nothing;
 * `ready` otherwise. The band's tiles, chart and breakdown each draw their own version of it.
 */
export type SectionState = 'loading' | 'ready' | 'empty' | 'unavailable';

interface SignalMeta {
  title: string;
  icon: string;
  description: string;
  unavailableHint: string;
  bucketLabel: string;
}

const META: Readonly<Record<StatsSignal, SignalMeta>> = {
  logs: {
    title: 'Logs', icon: 'list', description: 'ingest over the window · the log store',
    unavailableHint: 'Logs are available again once the server has restarted. Metrics and traces are unaffected.',
    bucketLabel: 'Open this bucket in Logs',
  },
  metrics: {
    title: 'Metrics', icon: 'chart-bar', description: 'storage written over the window · the metric store',
    unavailableHint: 'Metrics are available again once the server has restarted. Logs and traces are unaffected.',
    bucketLabel: 'Open this bucket in Metrics',
  },
  traces: {
    title: 'Traces', icon: 'activity', description: 'traces over the window · the trace store',
    unavailableHint: 'Traces are available again once the server has restarted. Logs and metrics are unaffected.',
    bucketLabel: 'Open this bucket in Traces',
  },
};

/**
 * One band of the Statistics page (design canvas, "Статистика 1920×1080"): the header with the
 * signal's name and its "Open …" link, then a grid of the eight tiles (window | store), the
 * bucket chart and the breakdown card with its two tabs. The band owns the chrome and the four
 * states; the section component behind it (Logs, Metrics, Traces) owns the numbers and projects
 * the breakdown's rows.
 */
@Component({
  selector: 'app-stats-section',
  imports: [LucideAngularModule, RouterLink],
  templateUrl: './stats-section.html',
  styleUrls: ['./stats-section.scss', './stats-section.responsive.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class StatsSectionComponent {
  readonly signal = input.required<StatsSignal>();
  /** The window column's caption: `Last 7 days`, `12 – 16 Oct`. */
  readonly caption = input.required<string>();
  readonly tiles = input.required<StatTile[]>();
  readonly state = input.required<SectionState>();
  readonly chartTitle = input.required<string>();
  readonly chartCaption = input<string>('');
  readonly legend = input<LegendItem[]>([]);
  readonly bars = input<ChartBar[]>([]);
  readonly ticks = input<AxisTick[]>([]);
  readonly emptyText = input<string>('Nothing in this window');
  readonly emptyHint = input<string>('');
  readonly emptyRowsText = input<string>('No data.');
  /** The store's own sentence when it refused, or what went wrong with the request. */
  readonly errorText = input<string>('');
  readonly tabs = input.required<BreakdownTab[]>();
  readonly tab = model.required<string>();
  readonly breakdownNote = input<string>('');
  /** The query parameters "Open …" carries: the same window in the page's own words. */
  readonly openParams = input<Params>({});

  readonly bucketOpen = output<ChartBar>();

  readonly meta = computed(() => META[this.signal()]);
  readonly route = computed(() => SIGNAL_ROUTE[this.signal()]);
  readonly headingId = computed(() => `stats-${this.signal()}`);

  /** The skeleton's shapes while loading: eight tiles, the bars' outline, six rows. */
  readonly skeletonTiles = [52, 70, 58, 50, 60, 48, 46, 62];
  readonly skeletonBars = Array.from({ length: 48 }, (_, i) => 30 + Math.round(30 * (1 + Math.sin(i / 5))) + ((i * 7) % 9));
  readonly skeletonRows = [90, 110, 70, 120, 60, 44];
}

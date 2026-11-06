import { ChangeDetectionStrategy, Component, computed, inject, signal } from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';

import { ViewportService } from '../../core/services/viewport.service';
import { DropdownService, OverlayPanelRef } from '../../shared/services/overlay';
import {
  DateDropdownController, DateRangeDropdownComponent,
} from '../../shared/components/date-range-dropdown/date-range-dropdown';
import { EmptyStateComponent, PageHeaderComponent } from '../../shared/components/ui';
import { QUICK_RANGES, QUICK_RANGE_LABELS } from '../../shared/utils/time-range';
import { StatsStore } from './stats.store';
import { StatsSignal } from './stats-range.util';
import { StatsLogsSectionComponent } from './sections/stats-logs';
import { StatsMetricsSectionComponent } from './sections/stats-metrics';
import { StatsTracesSectionComponent } from './sections/stats-traces';
import { StatsRangeSheetComponent } from './components/stats-range-sheet/stats-range-sheet';

/**
 * Statistics: logs, metrics and traces over one window, each a band of eight tiles, a bucket
 * chart and a breakdown (design canvas, "Статистика: логи, метрики и трейсы на одной странице").
 * The range is one control for the page — the six quick ranges and a Custom button that opens
 * the Logs page's own time range popover — and the bucket it chooses (15 min for a day, a day
 * for three months) is the grid every chart draws on. A phone shows one band at a time behind
 * three tabs, and the range in a sheet. Every number is an aggregate the server computes from
 * what it already keeps; no raw event, point or span reaches the page.
 */
@Component({
  selector: 'app-stats',
  imports: [
    LucideAngularModule, PageHeaderComponent, EmptyStateComponent,
    StatsLogsSectionComponent, StatsMetricsSectionComponent, StatsTracesSectionComponent, StatsRangeSheetComponent,
  ],
  providers: [StatsStore],
  templateUrl: './stats.html',
  styleUrls: ['./stats.scss', './stats.responsive.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class StatsComponent {
  readonly store = inject(StatsStore);
  readonly viewport = inject(ViewportService);
  private readonly dropdown = inject(DropdownService);

  readonly quickRanges = QUICK_RANGES;
  readonly signals: readonly { id: StatsSignal; label: string; icon: string }[] = [
    { id: 'logs', label: 'Logs', icon: 'list' },
    { id: 'metrics', label: 'Metrics', icon: 'chart-bar' },
    { id: 'traces', label: 'Traces', icon: 'activity' },
  ];

  readonly customOpen = signal(false);
  private customRef?: OverlayPanelRef<void, DateDropdownController>;

  /** The Custom button: its name until a custom window is applied, then the window itself. */
  readonly customLabel = computed(() =>
    this.store.preset() === 'custom' ? this.store.rangeLabel() : { text: 'Custom', extra: '' });

  /** The phone's range button: the quick range's short name, or the custom window. */
  readonly phoneRangeLabel = computed(() =>
    this.store.preset() === 'custom' ? this.store.rangeLabel().text : this.store.preset());

  /** Custom: the shared time range popover, anchored to its button, with the six quick ranges as its chips. */
  async openCustom(origin: HTMLElement): Promise<void> {
    if (this.customOpen()) { this.customRef?.close(); return; }
    this.customOpen.set(true);
    this.customRef = this.dropdown.open<DateRangeDropdownComponent, void, DateDropdownController>(
      DateRangeDropdownComponent,
      { origin, data: this.rangeController() },
    );
    await this.customRef.closed;
    this.customOpen.set(false);
    this.customRef = undefined;
  }

  private rangeController(): DateDropdownController {
    const s = this.store;
    return {
      presets: QUICK_RANGES.map(value => ({ label: QUICK_RANGE_LABELS[value], value })),
      timePreset: s.preset,
      customFrom: s.customFrom,
      customTo: s.customTo,
      setPreset: p => s.setPreset(p),
      applyRange: (from, to) => s.applyRange(from, to),
    };
  }
}

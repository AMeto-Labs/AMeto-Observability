import {
  Component, inject, signal, HostListener, ChangeDetectionStrategy,
} from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';

import { DropdownService, OverlayPanelRef } from '../../../../shared/services/overlay';
import {
  LevelsDropdownComponent, LevelsDropdownData, LevelsDropdownResult,
} from '../../dropdowns/levels-dropdown';
import {
  ServicesDropdownComponent, ServicesDropdownData, ServicesDropdownResult,
} from '../../dropdowns/services-dropdown';
import { DateRangeDropdownComponent, DateDropdownController } from '../../dropdowns/date-range-dropdown';
import { EventsStore } from '../../store/events.store';
import { TimePreset } from '../../store/events-filter.util';

/**
 * Filter options bar: date-range / levels / services dropdown triggers, the
 * per-level count chips, and the page-size selector. Each dropdown is a CDK
 * overlay opened via {@link DropdownService}; results are written back through
 * the shared {@link EventsStore}.
 */
@Component({
  selector: 'app-events-filter-bar',
  imports: [LucideAngularModule],
  templateUrl: './events-filter-bar.html',
  styleUrl: './events-filter-bar.scss',
  host: { style: 'display: contents' },
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class EventsFilterBarComponent {
  readonly store = inject(EventsStore);
  private dropdown = inject(DropdownService);

  private levelsRef?: OverlayPanelRef<LevelsDropdownResult, LevelsDropdownData>;
  private servicesRef?: OverlayPanelRef<ServicesDropdownResult, ServicesDropdownData>;
  private dateRef?: OverlayPanelRef<void, DateDropdownController>;

  levelsDropdownOpen  = signal(false);
  serviceDropdownOpen = signal(false);
  dateDropdownOpen    = signal(false);

  readonly levelItems = [
    { key: 'Verbose',     short: 'VRB' },
    { key: 'Debug',       short: 'DBG' },
    { key: 'Information', short: 'INF' },
    { key: 'Warning',     short: 'WRN' },
    { key: 'Error',       short: 'ERR' },
    { key: 'Fatal',       short: 'FTL' },
  ];
  readonly datePresets: { label: string; value: TimePreset }[] = [
    { label: 'Last 5 min',   value: '5m'     },
    { label: 'Last 15 min',  value: '15m'    },
    { label: 'Last 30 min',  value: '30m'    },
    { label: 'Last 1 day',   value: '1d'     },
    { label: 'Last 7 days',  value: '7d'     },
    { label: 'Last 2 weeks', value: '2w'     },
    { label: 'Last 1 month', value: '1mo'    },
    { label: 'Custom range', value: 'custom' },
  ];
  readonly calendarWeekDays = ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su'];
  readonly pageSizeOptions = [50, 100, 150, 300, 500];

  fmtCount(n: number | undefined): string {
    if (!n) return '0';
    return n >= 1000 ? `${(n / 1000).toFixed(1)}K` : String(n);
  }

  isLevelActive(level: string): boolean {
    return this.store.activeLevels().has(level);
  }

  /** Strips leading `-`/space from a date suggestion for display. */
  suggestionLabel(s: string): string {
    return s.replace(/^[-\s]+/, '');
  }

  onDateKeydown(e: Event): void {
    if ((e as KeyboardEvent).key === 'Enter') { e.preventDefault(); this.store.search(); }
  }

  async openDate(origin: HTMLElement): Promise<void> {
    if (this.dateDropdownOpen()) { this.dateRef?.close(); return; }
    this.closeAllDropdowns();
    this.store.openCalendar();
    this.dateDropdownOpen.set(true);
    this.dateRef = this.dropdown.open<DateRangeDropdownComponent, void, DateDropdownController>(
      DateRangeDropdownComponent,
      { origin, data: this.buildDateController() },
    );
    await this.dateRef.closed;
    this.dateDropdownOpen.set(false);
    this.dateRef = undefined;
  }

  async openLevels(origin: HTMLElement): Promise<void> {
    if (this.levelsDropdownOpen()) { this.levelsRef?.close(); return; }
    this.closeAllDropdowns();
    this.levelsDropdownOpen.set(true);
    this.levelsRef = this.dropdown.open<LevelsDropdownComponent, LevelsDropdownResult, LevelsDropdownData>(
      LevelsDropdownComponent,
      { origin, data: { active: this.store.activeLevels(), counts: this.store.levelCounts(), total: this.store.totalCount() } },
    );
    const result = await this.levelsRef.closed;
    this.levelsDropdownOpen.set(false);
    this.levelsRef = undefined;
    if (result) this.store.setLevels(result);
  }

  async openServices(origin: HTMLElement): Promise<void> {
    if (this.serviceDropdownOpen()) { this.servicesRef?.close(); return; }
    this.closeAllDropdowns();
    await this.store.loadBackendServices();
    this.serviceDropdownOpen.set(true);
    this.servicesRef = this.dropdown.open<ServicesDropdownComponent, ServicesDropdownResult, ServicesDropdownData>(
      ServicesDropdownComponent,
      { origin, data: {
        selected: this.store.selectedServices(),
        services: this.store.availableServices(),
        counts: this.store.serviceCounts(),
        total: this.store.totalCount(),
      } },
    );
    const result = await this.servicesRef.closed;
    this.serviceDropdownOpen.set(false);
    this.servicesRef = undefined;
    if (result) this.store.setServices(result);
  }

  /** The date range shares state with the toolbar/URL, so it stays store-owned; the
   *  dropdown is a pure view over these signals + actions. */
  private buildDateController(): DateDropdownController {
    const s = this.store;
    return {
      presets: this.datePresets,
      weekDays: this.calendarWeekDays,
      timePreset: s.timePreset,
      customFrom: s.customFrom,
      customTo: s.customTo,
      fromSuggestion: s.customFromSuggestion,
      toSuggestion: s.customToSuggestion,
      calPickingEnd: s.calPickingEnd,
      monthLabel: s.calendarMonthLabel,
      days: s.calendarDays,
      customFromValid: s.customFromValid,
      customToValid: s.customToValid,
      canSearch: s.canSearch,
      setPreset: (p) => s.setTimePreset(p),
      prevMonth: () => s.prevCalendarMonth(),
      nextMonth: () => s.nextCalendarMonth(),
      selectDay: (day) => s.selectCalendarDay(day),
      setFrom: (v) => s.setFrom(v),
      setTo: (v) => s.setTo(v),
      setFromSuggestion: (v) => s.setFromSuggestion(v),
      setToSuggestion: (v) => s.setToSuggestion(v),
      acceptFromSuggestion: () => s.acceptFromSuggestion(),
      acceptToSuggestion: () => s.acceptToSuggestion(),
      suggestionLabel: (str) => this.suggestionLabel(str),
      onDateKeydown: (e) => this.onDateKeydown(e),
      apply: () => s.search(),
    };
  }

  @HostListener('document:keydown.escape')
  closeDropdownsOnEscape(): void {
    this.closeAllDropdowns();
  }

  private closeAllDropdowns(): void {
    this.levelsRef?.close();
    this.servicesRef?.close();
    this.dateRef?.close();
  }
}

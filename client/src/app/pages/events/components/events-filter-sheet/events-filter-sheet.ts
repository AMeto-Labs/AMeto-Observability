import {
  ChangeDetectionStrategy, Component, OnInit, computed, inject, signal,
} from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';

import { BottomSheetComponent } from '../../../../shared/components/bottom-sheet/bottom-sheet';
import { LEVELS } from '../../../../core/models/event.model';
import { serviceColor } from '../../../../shared/utils/service-color';
import { EventsStore } from '../../store/events.store';
import {
  DEFAULT_PRESET, LEVEL_ITEMS, PRESET_LABELS, TimePreset, fmtPoint, parseCustomDate, presetRange,
} from '../../store/events-filter.util';

/** How many services the sheet lists before "Show N more", as the services dropdown does. */
const SERVICES_SHOWN = 8;

/** `yyyy-MM-dd HH:mm` (the store's) → `yyyy-MM-ddTHH:mm` (a datetime-local input's), and back. */
export function toInputDateTime(v: string): string {
  return v ? v.replace(' ', 'T').slice(0, 16) : '';
}
export function fromInputDateTime(v: string): string {
  return v ? v.replace('T', ' ').slice(0, 16) : '';
}

/**
 * The phone's filter sheet (design canvas, "Телефон — лист фильтров логов"): the time range,
 * the levels, the services, the page size and the Full messages switch in one sheet, in place
 * of the filter bar's three dropdowns and its select. Everything is a DRAFT until Apply — as in
 * the dropdowns — and Apply hands the whole draft to the store for one load
 * ({@link EventsStore.applyFilterSheet}). Clear is the toolbar's Clear (resetAll) and keeps the
 * sheet open, as the Traces sheet's Clear does.
 */
@Component({
  selector: 'app-events-filter-sheet',
  imports: [LucideAngularModule, BottomSheetComponent],
  templateUrl: './events-filter-sheet.html',
  styleUrl: './events-filter-sheet.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class EventsFilterSheetComponent implements OnInit {
  readonly store = inject(EventsStore);

  readonly levelItems = LEVEL_ITEMS;
  readonly presets = Object.keys(PRESET_LABELS) as Exclude<TimePreset, 'custom'>[];
  readonly pageSizes = [50, 100, 150, 300, 500];
  readonly serviceColor = serviceColor;

  // ── The draft ───────────────────────────────────────────────────────
  readonly preset = signal<TimePreset>(this.store.timePreset());
  readonly from = signal(toInputDateTime(this.store.customFrom()));
  readonly to = signal(toInputDateTime(this.store.customTo()));
  readonly levels = signal<ReadonlySet<string>>(new Set(this.store.activeLevels()));
  readonly services = signal<ReadonlySet<string>>(new Set(this.store.selectedServices()));
  readonly pageSize = signal(this.store.pageSize());
  readonly wrap = signal(this.store.wrapMessages());

  readonly svcSearch = signal('');
  readonly showMore = signal(false);

  readonly allLevels = computed(() => this.levels().size === LEVELS.length);

  /** The window the draft would load: a preset resolved now, or the custom bounds. */
  readonly rangeSummary = computed(() => {
    const now = new Date();
    const p = this.preset();
    if (p !== 'custom') {
      const from = parseCustomDate(presetRange(p, now).from);
      return from ? `${fmtPoint(from, now)} – now` : '';
    }
    const from = parseCustomDate(fromInputDateTime(this.from()));
    const to = parseCustomDate(fromInputDateTime(this.to()));
    if (!from) return 'Pick a start';
    return `${fmtPoint(from, now)} – ${to ? fmtPoint(to, now) : 'now'}`;
  });

  /** A custom window needs a start, and an end after it. */
  readonly rangeValid = computed(() => {
    if (this.preset() !== 'custom') return true;
    const from = parseCustomDate(fromInputDateTime(this.from()));
    const toRaw = fromInputDateTime(this.to());
    const to = toRaw ? parseCustomDate(toRaw) : null;
    return !!from && (!toRaw || (!!to && to > from));
  });

  readonly filteredServices = computed(() => {
    const q = this.svcSearch().trim().toLowerCase();
    const all = this.store.availableServices();
    return q ? all.filter(s => s.toLowerCase().includes(q)) : all;
  });

  readonly shownServices = computed(() =>
    this.showMore() ? this.filteredServices() : this.filteredServices().slice(0, SERVICES_SHOWN));

  readonly hiddenServiceCount = computed(() =>
    this.showMore() ? 0 : Math.max(0, this.filteredServices().length - SERVICES_SHOWN));

  readonly servicesSummary = computed(() => {
    const n = this.services().size;
    const total = this.store.availableServices().length;
    return n === 0 ? `All of ${total}` : `${n} of ${total} selected`;
  });

  ngOnInit(): void {
    // The full service list is fetched on first use, as the services dropdown fetches it.
    void this.store.loadBackendServices();
  }

  pickPreset(p: TimePreset): void {
    this.preset.set(p);
    if (p === 'custom' && !this.from()) {
      // Custom starts from where the window is now, so the fields are not blank.
      this.from.set(toInputDateTime(this.store.customFrom()));
      this.to.set(toInputDateTime(this.store.customTo()));
    }
  }

  setFrom(v: string): void { this.from.set(v); this.preset.set('custom'); }
  setTo(v: string): void { this.to.set(v); this.preset.set('custom'); }

  isLevel(level: string): boolean { return this.levels().has(level); }

  toggleLevel(level: string): void {
    this.levels.update(set => {
      const next = new Set(set);
      if (next.has(level)) next.delete(level); else next.add(level);
      return next;
    });
  }

  allLevelsOn(): void {
    this.levels.set(new Set(LEVELS as readonly string[]));
  }

  isService(svc: string): boolean { return this.services().has(svc); }

  toggleService(svc: string): void {
    this.services.update(set => {
      const next = new Set(set);
      if (next.has(svc)) next.delete(svc); else next.add(svc);
      return next;
    });
  }

  servicePercent(svc: string): string {
    const total = this.store.totalCount();
    if (!total) return '0 %';
    return `${Math.round(((this.store.serviceCounts()[svc] ?? 0) / total) * 100)} %`;
  }

  fmtCount(n: number | undefined): string {
    if (!n) return '0';
    return n >= 1000 ? `${(n / 1000).toFixed(1)}K` : String(n);
  }

  /** The toolbar's Clear: the filter text, the levels and services it carried, and the window. */
  clear(): void {
    this.store.resetAll();
    this.preset.set(DEFAULT_PRESET);
    this.from.set('');
    this.to.set('');
    this.levels.set(new Set(LEVELS as readonly string[]));
    this.services.set(new Set());
  }

  apply(): void {
    if (!this.rangeValid()) return;
    this.store.applyFilterSheet({
      preset: this.preset(),
      from: fromInputDateTime(this.from()),
      to: fromInputDateTime(this.to()),
      levels: this.levels(),
      services: this.services(),
      pageSize: this.pageSize(),
      wrap: this.wrap(),
    });
  }

  close(): void {
    this.store.closeFilterSheet();
  }
}
